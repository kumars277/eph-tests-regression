package com.eph.automation.testing.services.db.BCS_ETLCoreSQL;




public class BCS_ETLCoreDataChecksSQL {

    public static String GET_RANDOM_ACCPROD_KEY_INBOUND =
           "SELECT sourceref \n" +
                   "FROM\n" +
                   "  (\n" +
                   "(\n" +
                   "      SELECT DISTINCT \n" +
                   "        NULLIF(sourceref, '') sourceref \n" +
                   "      , NULLIF(CAST(accountableproduct AS varchar), '') accountableproduct \n" +
                   "      , NULLIF(accountablename, '') accountablename \n" +
                   "      , NULLIF(accountableparent, '') accountableparent \n" +
                   "      , concat(NULLIF(sourceref, ''), NULLIF(accountableparent, '')) u_key \n" +
                   "      , 'N' dq_err \n" +
                   "      FROM \n" +
                   "        ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification\n " +
                   "      LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".worktypecode ON (split_part(classification.value, ' | ', 1) = ppmcode)) \n" +
                   "      WHERE (classification.classificationcode = 'DCDFAC | Accounting class') \n" +
                   "   ) )  A \n" +
                   "WHERE ((A.sourceref IS NOT NULL) AND (A.accountableparent IS NOT NULL)) order by rand() limit %s";

    public static String GET_RANDOM_WRK_PERS_KEY_INBOUND =
            "SELECT u_key as sourceref\n" +
                    "FROM \n" +
                    "  ( \n" +
                    "   SELECT DISTINCT \n" +
                    "     NULLIF(sourceref, '') worksourceref \n" +
                    "   , NULLIF(CAST(businesspartnerid AS varchar), '') personsourceref \n" +
                    "   , NULLIF(rolecode.ephcode, '') roletype \n" +
                    "   , concat(concat(NULLIF(sourceref, ''), NULLIF(rolecode.ephcode, '')), NULLIF(CAST(businesspartnerid AS varchar), '')) u_key \n" +
                    "   , NULLIF(CAST(sequence AS varchar), '') sequence \n" +
                    "   , NULLIF(CAST(locationid AS varchar), '') deduplicator \n" +
                    "   , date_parse(NULLIF(metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , 'N' dq_err \n" +
                    "   FROM \n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originators \n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(copyrightholdertype, ' | ', 1) = rolecode.ppmcode))\n" +
                    "UNION    SELECT\n" +
                    "     NULLIF(sourceref, '') worksourceref \n" +
                    "   , NULLIF(personid, '') personsourceref \n" +
                    "   , NULLIF(rolecode.ephcode, '') roletype \n" +
                    "   , concat(concat(sourceref, rolecode.ephcode), personid) u_key \n" +
                    "   , (CASE WHEN (substr(split_part(NULLIF(responsibility, ''), ' | ', 1), -1, 1) = '2') THEN '2' ELSE '1' END) sequence \n" +
                    "   , '0' deduplicator \n" +
                    "   , date_parse(NULLIF(metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , 'N' dq_err \n" +
                    "   FROM \n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_responsibilities\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(responsibility, ' | ', 1) = rolecode.ppmcode))\n" +
                    ")  A \n" +
                    "WHERE (((A.worksourceref IS NOT NULL) AND \n" +
                    "(A.personsourceref IS NOT NULL)) AND (A.roletype IS NOT NULL))\n" +
                    "order by rand() limit %s";

    public static String GET_RANDOM_WORK_KEY_INBOUND =
            "SELECT sourceref\n" +
                    "FROM\n" +
                    "  (\n" +
                    "   SELECT DISTINCT\n" +
                    "     NULLIF(content.sourceref, '') sourceref \n" +
                    "   , NULLIF(content.title, '') title\n" +
                    "   , NULLIF(content.subtitle, '') subtitle \n" +
                    "   , NULLIF(content.volumeno, '') volumeno\n" +
                    "   , content.copyrightyear copyrightyear\n" +
                    "   , content.editionno editionno \n" +
                    "   , substring (split_part(NULLIF(content.subgroup, ''), ' | ', 1), 2, 3) pmc \n" +
                    "   , COALESCE(NULLIF(worktypecode.ephcode, ''), 'UNK') work_type \n" +
                    "   , COALESCE(NULLIF(status.ephworkcode, ''), 'UNK') statuscode \n" +
                    "   , split_part(NULLIF(content.imprint, ''), ' | ', 1) imprintcode \n" +
                    "   , NULLIF(cast(opcocode.\"11icode\" as varchar), '') te_opcoc \n" +
                    "   , NULLIF(cast(opcocode.r12code as varchar), '') opcov \n" +
                    "   , NULLIF(cast(temapping.mapped_r12_sgmnt_value as varchar), '') resp_centre\n" +
                    "   , substring(split_part(NULLIF(content.division, ''), ' | ', 1), 2, 3) pmg\n" +
                    "   , NULLIF(languagecode.ephcode, '') languagecodev\n" +
                    "   , CAST(NULL AS boolean) electro_rights_indicator\n" +
                    "   , CAST(NULL AS varchar) f_oa_journal_typev\n" +
                    "   , CAST(NULL AS varchar) f_society_ownershipv\n" +
                    "   , CAST(NULL AS varchar) subscription_type \n" +
                    "   , date_parse(NULLIF(content.metamodifiedon, ''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   FROM\n" +
                    "     (((((("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily versionfamily ON (content.sourceref = versionfamily.workmasterprojectno))\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification ON ((content.sourceref = classification.sourceref) AND (classification.classificationcode = 'DCDFAC | Accounting class')))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT DISTINCT \n" +
                    "        masterstatus.sourceref \n" +
                    "      , statuscode.ephworkcode \n" +
                    "      FROM \n" +
                    "        ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode \n" +
                    "      INNER JOIN (\n" +
                    "         SELECT\n" +
                    "           workmasterprojectno sourceref \n" +
                    "         , min(priority) statuspriority \n" +
                    "         FROM \n" +
                    "           (("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily versionfamily\n" +
                    "         INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation ON (versionfamily.sourceref = sublocation.sourceref))\n" +
                    "         INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (split_part(sublocation.status, ' | ', 1) = statuscode.ppmcode))\n" +
                    "         GROUP BY workmasterprojectno \n" +
                    "      )  masterstatus ON (statuscode.priority = masterstatus.statuspriority))\n" +
                    "   )  status ON (content.sourceref = status.sourceref))\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".opcocode opcocode ON (content.ownership = opcocode.ppmcode)\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".temapping temapping ON ((opcocode.\"11icode\" = temapping.r11_sgmnt_c_value) and (substring(split_part(NULLIF(content.division, ''), ' | ', 1), 2, 3) = temapping.r11_sgmnt_a_value) and (temapping.r11_sgmnt_b_value = '0') and (NULLIF(temapping.r11_sgmnt_d_value,'') is null) and (temapping.mapping_rule_number = 2)))\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".languagecode languagecode ON (split_part(content.language, ' | ', 1) = languagecode.ppmcode))\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".worktypecode worktypecode ON (split_part(classification.value, ' | ', 1) = worktypecode.ppmcode))\n" +
                    ")  A \n" +
                    "WHERE (A.sourceref IS NOT NULL) order by rand() limit %s";

    public static String GET_WORK_INBOUND_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",subtitle as SUBTITLE \n" +
                    ",volumeno as VOLUMENO \n" +
                    ",copyrightyear as COPYRIGHTYEAR \n" +
                    ",editionno as EDITIONNO \n" +
                    ",pmc as PMC \n" +
                    ",work_type as WORKTYPE \n" +
                    ",statuscode as STATUSCODE \n" +
                    ",imprintcode as IMPRINTCODE \n" +
                    ",te_opcoc as TEOPCO \n" +
                    ",opcov as OPCO \n" +
                    ",resp_centre as RESPCENTRE \n" +
                    ",pmg as PMG \n" +
                    ",languagecodev as LANGUAGECODE \n" +
                    ",electro_rights_indicator as ELECTRORIGHTSINDICATOR \n" +
                    ",f_oa_journal_typev as FOAJOURNALTYPE \n" +
                    ",f_society_ownershipv as FSOCIETYOWNERSHIP \n" +
                    ",subscription_type as SUBSCRIPTIONTYPE \n" +
                    "FROM\n" +
             "  (\n" +
             "   SELECT DISTINCT\n" +
             "     NULLIF(content.sourceref, '') sourceref \n" +
             "   , NULLIF(content.title, '') title\n" +
             "   , NULLIF(content.subtitle, '') subtitle \n" +
             "   , NULLIF(content.volumeno, '') volumeno\n" +
             "   , content.copyrightyear copyrightyear\n" +
             "   , content.editionno editionno \n" +
             "   , substring (split_part(NULLIF(content.subgroup, ''), ' | ', 1), 2, 3) pmc \n" +
             "   , COALESCE(NULLIF(worktypecode.ephcode, ''), 'UNK') work_type \n" +
             "   , COALESCE(NULLIF(status.ephworkcode, ''), 'UNK') statuscode \n" +
             "   , split_part(NULLIF(content.imprint, ''), ' | ', 1) imprintcode \n" +
             "   , NULLIF(cast(opcocode.\"11icode\" as varchar), '') te_opcoc \n" +
             "   , NULLIF(cast(opcocode.r12code as varchar), '') opcov \n" +
             "   , NULLIF(cast(temapping.mapped_r12_sgmnt_value as varchar), '') resp_centre\n" +
             "   , substring(split_part(NULLIF(content.division, ''), ' | ', 1), 2, 3) pmg\n" +
             "   , NULLIF(languagecode.ephcode, '') languagecodev\n" +
             "   , CAST(NULL AS boolean) electro_rights_indicator\n" +
             "   , CAST(NULL AS varchar) f_oa_journal_typev\n" +
             "   , CAST(NULL AS varchar) f_society_ownershipv\n" +
             "   , CAST(NULL AS varchar) subscription_type \n" +
             "   , date_parse(NULLIF(content.metamodifiedon, ''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
             "   FROM\n" +
             "     (((((("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
             "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily versionfamily ON (content.sourceref = versionfamily.workmasterprojectno))\n" +
             "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification ON ((content.sourceref = classification.sourceref) AND (classification.classificationcode = 'DCDFAC | Accounting class')))\n" +
             "   LEFT JOIN (\n" +
             "      SELECT DISTINCT \n" +
             "        masterstatus.sourceref \n" +
             "      , statuscode.ephworkcode \n" +
             "      FROM \n" +
             "        ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode \n" +
             "      INNER JOIN (\n" +
             "         SELECT\n" +
             "           workmasterprojectno sourceref \n" +
             "         , min(priority) statuspriority \n" +
             "         FROM \n" +
             "           (("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily versionfamily\n" +
             "         INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation ON (versionfamily.sourceref = sublocation.sourceref))\n" +
             "         INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (split_part(sublocation.status, ' | ', 1) = statuscode.ppmcode))\n" +
             "         GROUP BY workmasterprojectno \n" +
             "      )  masterstatus ON (statuscode.priority = masterstatus.statuspriority))\n" +
             "   )  status ON (content.sourceref = status.sourceref))\n" +
             "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".opcocode opcocode ON (content.ownership = opcocode.ppmcode)\n" +
             "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".temapping temapping ON ((opcocode.\"11icode\" = temapping.r11_sgmnt_c_value) and (substring(split_part(NULLIF(content.division, ''), ' | ', 1), 2, 3) = temapping.r11_sgmnt_a_value) and (temapping.r11_sgmnt_b_value = '0') and (NULLIF(temapping.r11_sgmnt_d_value,'') is null) and (temapping.mapping_rule_number = 2)))\n" +
             "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".languagecode languagecode ON (split_part(content.language, ' | ', 1) = languagecode.ppmcode))\n" +
             "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".worktypecode worktypecode ON (split_part(classification.value, ' | ', 1) = worktypecode.ppmcode))\n" +
             ")  A \n" +
             "WHERE (A.sourceref IS NOT NULL) and sourceref in ('%s') order by sourceref desc";

    public static String GET_WORK_PERS_INBOUND_DATA =
            "select " +
                    "worksourceref as WORKSOURCEREF \n" +
                    ",personsourceref as PERSONSOURCEREF \n" +
                    ",roletype as ROLETYPE \n" +
                    ",u_key as UKEY \n" +
                    ",sequence as SEQUENCE \n" +
                    ",deduplicator as DEDUPLICATOR \n" +
                    ",dq_err as DQ_ERR \n" +
                    "FROM \n" +
                    "  ( \n" +
                    "   SELECT DISTINCT \n" +
                    "     NULLIF(sourceref, '') worksourceref \n" +
                    "   , NULLIF(CAST(businesspartnerid AS varchar), '') personsourceref \n" +
                    "   , NULLIF(rolecode.ephcode, '') roletype \n" +
                    "   , concat(concat(NULLIF(sourceref, ''), NULLIF(rolecode.ephcode, '')), NULLIF(CAST(businesspartnerid AS varchar), '')) u_key \n" +
                    "   , NULLIF(CAST(sequence AS varchar), '') sequence \n" +
                    "   , NULLIF(CAST(locationid AS varchar), '') deduplicator \n" +
                    "   , date_parse(NULLIF(metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , 'N' dq_err \n" +
                    "   FROM \n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originators \n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(copyrightholdertype, ' | ', 1) = rolecode.ppmcode))\n" +
                    "UNION    SELECT\n" +
                    "     NULLIF(sourceref, '') worksourceref \n" +
                    "   , NULLIF(personid, '') personsourceref \n" +
                    "   , NULLIF(rolecode.ephcode, '') roletype \n" +
                    "   , concat(concat(sourceref, rolecode.ephcode), personid) u_key \n" +
                    "   , (CASE WHEN (substr(split_part(NULLIF(responsibility, ''), ' | ', 1), -1, 1) = '2') THEN '2' ELSE '1' END) sequence \n" +
                    "   , '0' deduplicator \n" +
                    "   , date_parse(NULLIF(metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , 'N' dq_err \n" +
                    "   FROM \n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_responsibilities\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(responsibility, ' | ', 1) = rolecode.ppmcode))\n" +
                    ")  A \n" +
                    "WHERE (((A.worksourceref IS NOT NULL) AND \n" +
                    "(A.personsourceref IS NOT NULL)) AND (A.roletype IS NOT NULL))\n" +
                    "and u_key in ('%s') order by u_key desc";


    public static String GET_ACCPROD_REC_INBOUND_DATA =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "FROM\n" +
                    "  (\n" +
                    "(\n" +
                    "      SELECT DISTINCT \n" +
                    "        NULLIF(sourceref, '') sourceref \n" +
                    "      , NULLIF(CAST(accountableproduct AS varchar), '') accountableproduct \n" +
                    "      , NULLIF(accountablename, '') accountablename \n" +
                    "      , NULLIF(accountableparent, '') accountableparent \n" +
                    "      , concat(NULLIF(sourceref, ''), NULLIF(accountableparent, '')) u_key \n" +
                    "      , 'N' dq_err \n" +
                    "      FROM \n" +
                    "        ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification\n" +
                    "      LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".worktypecode ON (split_part(classification.value, ' | ', 1) = ppmcode))\n" +
                    "      WHERE (classification.classificationcode = 'DCDFAC | Accounting class') \n" +
                    "   ) )  A \n" +
                    "WHERE ((A.sourceref IS NOT NULL) AND (A.accountableparent IS NOT NULL)) and sourceref in ('%s') order by sourceref desc";




    public static String GET_RANDOM_MANIF_KEY_INBOUND =
            "select sourceref from( \n" +
                    "SELECT DISTINCT product.sourceref, content.title, (CASE WHEN (intedition.classificationcode IS NULL) \n" +
                    "THEN false ELSE true END) intereditionflag, cast((date_parse(COALESCE(NULLIF(firstactual,''),\n" +
                    "NULLIF(firstplanned,'')),'%%d-%%b-%%Y')) as date ) firstpublisheddate, product.binding, \n" +
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
                    ") order by rand() limit %s";

    public static String GET_MANIF_INBOUND_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",intereditionflag as INTEREDITIONFLAG \n" +
                    ",firstpublisheddate as FIRSTPUBLISHEDDATE \n" +
                    ",binding as BINDING \n" +
                    ",manifestation_type as MANIFESTATIONTYPE \n" +
                    ",status as STATUS \n" +
                    ",work_id as WORKID \n" +
                    ",last_pub_date as LASTPUBDATE \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from(\n" +
                    "SELECT DISTINCT product.sourceref, content.title, (CASE WHEN (intedition.classificationcode IS NULL) \n" +
                    "THEN false ELSE true END) intereditionflag, cast((date_parse(COALESCE(NULLIF(firstactual,''),\n" +
                    "NULLIF(firstplanned,'')),'%%d-%%b-%%Y')) as date ) firstpublisheddate, product.binding, \n" +
                    " manifestationtypecode.ephcode manifestation_type, manifestationstatus.ephmanifestationcode status \n" +
                    "   , workprod.workmasterprojectno work_id, CAST(NULL AS timestamp) last_pub_date, 'N' dq_err \n" +
                    "   from ((((((("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content ON (product.sourceref = content.sourceref))\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily workprod ON ((product.sourceref = workprod.sourceref) AND (workprod.workmasterprojectno IS NOT NULL)))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT sourceref, classificationcode \n" +
                    "      FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification\n" +
                    "      WHERE (classificationcode LIKE 'PARELIE%')\n" +
                    "   ) intedition ON (product.sourceref = intedition.sourceref))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT sourceref, min(plannedpubdate) firstplanned \n" +
                    "      FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation\n" +
                    "      WHERE (plannedpubdate <> '') GROUP BY sourceref \n" +
                    "   ) planneddates ON (product.sourceref = planneddates.sourceref))\n" +
                    "   LEFT JOIN (\n" +
                    "      select sourceref, min(pubdateactual) firstactual \n" +
                    "      FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation\n" +
                    "      WHERE (pubdateactual <> '') GROUP BY sourceref \n" +
                    "   )  actualdates ON (product.sourceref = actualdates.sourceref))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT distinct sourceref, ephmanifestationcode \n" +
                    "      FROM ((SELECT sourceref, min(priority) statuspriority \n" +
                    "         FROM ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation\n" +
                    "         INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (split_part(status, ' | ', 1) = ppmcode))\n" +
                    "         GROUP BY sourceref)  masterstatus\n" +
                    "      INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (statuspriority = priority))\n" +
                    "   )  manifestationstatus ON (product.sourceref = manifestationstatus.sourceref))\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".manifestationtypecode ON (split_part(product.versiontype, ' | ', 1) = manifestationtypecode.ppmcode))\n" +
                    ") where sourceref in ('%s') order by sourceref desc";


    public static String GET_RANDOM_PERSON_KEY_INBOUND =
            "SELECT sourceref \n" +
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
                    " WHERE (A.sourceref IS NOT NULL) order by rand() limit %s";

    public static String GET_PERSON_INBOUND_DATA =
        "select " +
                "sourceref as SOURCEREF" +
                ",firstname as FIRSTNAME" +
                ",familyname as FAMILYNAME" +
                ",peoplehub_id as PEOPLEHUBID" +
                ",email_address as EMAIL" +
                ",dq_err as DQ_ERR " +
                "FROM( \n" +
                "   SELECT DISTINCT\n" +
                "     businesspartnerid sourceref \n" +
                "   , (CASE WHEN (isperson = 'N') THEN NULLIF(department, '') ELSE NULLIF(firstname, '') END) firstname \n" +
                "   , (CASE WHEN (isperson = 'N') THEN NULLIF(institution, '') ELSE NULLIF(lastname, '') END) familyname \n" +
                "   , CAST(null AS varchar) peoplehub_id \n" +
                "   , CAST(null AS varchar) email_address \n" +
                "   , 'N' dq_err \n" +
                "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originators \n" +
                ")  A \n" +
                "WHERE (A.sourceref IS NOT NULL) and sourceref in (%s) order by sourceref desc";


    public static String GET_RANDOM_WRK_RELT_KEY_INBOUND =
            "select sourceref from( \n" +
                    "SELECT \n" +
                    "     concat(concat(CAST(sourceref AS varchar), split_part(relationtype, ' | ', 1)), CAST(projectno AS varchar)) sourceref \n" +
                    "   , sourceref parentref \n" +
                    "   , projectno childref \n" +
                    "   , relationtypecode.ephcode relationtyperef \n" +
                    "   , date_parse(metamodifiedon,'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , 'N' dq_err \n" +
                    "   FROM \n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_relations \n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".relationtypecode ON \n" +
                    "   (split_part(relationtype, ' | ', 1) = relationtypecode.ppmcode))) order by rand() limit %s";

    public static String GET_WORK_RELT_INBOUND_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",dq_err as DQ_ERR from( \n" +
                    "SELECT \n" +
                    "     concat(concat(CAST(sourceref AS varchar), split_part(relationtype, ' | ', 1)), CAST(projectno AS varchar)) sourceref \n" +
                    "   , sourceref parentref \n" +
                    "   , projectno childref \n" +
                    "   , relationtypecode.ephcode relationtyperef \n" +
                    "   , date_parse(metamodifiedon,'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , 'N' dq_err \n" +
                    "   FROM \n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_relations \n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".relationtypecode ON \n" +
                    "   (split_part(relationtype, ' | ', 1) = relationtypecode.ppmcode))) where sourceref in ('%s') order by sourceref desc";



    public static String GET_RANDOM_ACCPROD_KEY_CURRENT =
            "SELECT u_key as u_key \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_current_v \n" +
                    " order by rand() limit %s";

    public static String GET_RANDOM_MANIF_IDENT_KEY_INBOUND =
            "select sourceref from(\n" +
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
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content \n" +
                    "   WHERE (seriesissn <> '') \n" +
                    "   )order by rand() limit %s";

    public static String GET_RANDOM_WORK_IDENT_KEY_INBOUND =
            "WITH \n" +
                    "  work_id AS (\n" +
                    "   SELECT DISTINCT \"workmasterprojectno\"\n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily \n" +
                    ") \n" +
                    "select sourceref from( \n" +
                    "SELECT \n" +
                    "  sourceref \n" +
                    ", piidack identifier \n" +
                    ", 'DAC-Key' identifier_type \n" +
                    "FROM \n" +
                    "  ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content \n" +
                    " INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                    "WHERE (piidack <> '') \n" +
                    "UNION ALL SELECT\n" +
                    "  sourceref \n" +
                    ", doi identifier \n" +
                    ", 'DOI' identifier_type \n" +
                    "FROM \n" +
                    "  ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
                    "INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno)) \n" +
                    "WHERE (doi <> '')\n" +
                    "UNION ALL SELECT\n" +
                    "  sourceref \n" +
                    ", seriesissn identifier \n" +
                    ", 'ISSN-L' identifier_type \n" +
                    "FROM \n" +
                    "  ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
                    "INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                    "WHERE (seriesissn <> '')\n" +
                    "UNION ALL SELECT\n" +
                    "  sourceref \n" +
                    ", orderno identifier \n" +
                    ", 'PPM-PART' identifier_type \n" +
                    "FROM \n" +
                    "  ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                    "INNER JOIN work_id ON (product.sourceref = work_id.workmasterprojectno))\n" +
                    "WHERE (orderno <> '')) order by rand() limit %s";

    public static String GET_WORK_IDENT_INBOUND_DATA =
              "WITH \n" +
                      "  work_id AS (\n" +
                      "   SELECT DISTINCT \"workmasterprojectno\"\n" +
                      "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily \n" +
                      ") \n" +
                      "select \n" +
                      "sourceref as SOURCEREF \n" +
                      ",identifier as IDENTIFIER \n" +
                      ",identifier_type as IDENTIFIERTYPE \n" +
                      "from( \n" +
                      "SELECT \n" +
                      "  sourceref \n" +
                      ", piidack identifier \n" +
                      ", 'DAC-Key' identifier_type \n" +
                      "FROM \n" +
                      "  ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content \n" +
                      " INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                      "WHERE (piidack <> '') \n" +
                      "UNION ALL SELECT\n" +
                      "  sourceref \n" +
                      ", doi identifier \n" +
                      ", 'DOI' identifier_type \n" +
                      "FROM \n" +
                      "  ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
                      "INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno)) \n" +
                      "WHERE (doi <> '')\n" +
                      "UNION ALL SELECT\n" +
                      "  sourceref \n" +
                      ", seriesissn identifier \n" +
                      ", 'ISSN-L' identifier_type \n" +
                      "FROM \n" +
                      "  ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
                      "INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                      "WHERE (seriesissn <> '')\n" +
                      "UNION ALL SELECT\n" +
                      "  sourceref \n" +
                      ", orderno identifier \n" +
                      ", 'PPM-PART' identifier_type \n" +
                      "FROM \n" +
                      "  ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                      "INNER JOIN work_id ON (product.sourceref = work_id.workmasterprojectno))\n" +
                      "WHERE (orderno <> '')) where sourceref in ('%s') order by sourceref,identifier,identifier_type desc";

    public static String GET_MANIF_IDENT_INBOUND_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from ( \n" +
                    "SELECT\n" +
                    "     sourceref \n" +
                    "   , isbn13 identifier \n" +
                    "   , 'ISBN' identifier_type \n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product \n" +
                    "   WHERE (isbn13 <> '') \n" +
                    " UNION ALL  SELECT\n" +
                    "     sourceref \n" +
                    "   , seriesissn identifier \n" +
                    "   , 'ISSN' identifier_type \n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content \n" +
                    "   WHERE (seriesissn <> '') \n" +
                    "   )where sourceref in ('%s') order by sourceref,identifier,identifier_type desc";

    public static String GET_MANIF_IDENT_CURR_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_current_v where sourceref in ('%s') order by sourceref,identifier,identifier_type desc";

    public static String GET_WORK_IDENT_CURR_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_current_v where sourceref in ('%s') order by sourceref,identifier,identifier_type desc";

    public static String GET_PERSON_CURR_DATA =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQ_ERR " +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_current_v where sourceref in \n" +
                    "(%s) order by sourceref desc";


    public static String GET_ACCPROD_REC_CURR_REC =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_current_v \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_ACCPROD_REC_CURR_DATA =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_current_v \n" +
                    "where sourceref in ('%s') \n" +
                    "order by sourceref desc";



    public static String GET_ACCPROD_REC_CURR_HIST_DATA =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_part  where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_ACCPROD_REC_TRANS_FILE_DATA =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part  where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_ACCPROD_REC_DELTA_CURRENT =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_accountable_product \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_ACCPROD_REC_DELTA_CURRENT_HIST =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_accountable_product_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_accountable_product_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_KEY_ACCPROD_DELTA_CURRENT_HIST =
            "SELECT u_key as UKEY \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_accountable_product_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_accountable_product_part)" +
                    " order by rand() limit %s";


    public static String GET_RANDOM_MANIF_KEY_CURRENT =
            "SELECT u_key as u_key \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_current_v \n" +
                    " order by rand() limit %s";

    public static String GET_MANIF_CURR_REC =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",title as TITLE \n" +
                    ",intereditionflag as INTEREDITIONFLAG \n" +
                    ",firstpublisheddate as FIRSTPUBLISHEDDATE \n" +
                    ",binding as BINDING \n" +
                    ",manifestation_type as MANIFESTATIONTYPE \n" +
                    ",status as STATUS \n" +
                    ",work_id as WORKID \n" +
                    ",last_pub_date as LASTPUBDATE \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_current_v where u_key in ('%s') order by u_key desc";



    public static String GET_MANIF_REC_DELTA_CURRENT =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",title as TITLE \n" +
                    ",intereditionflag as INTEREDITIONFLAG \n" +
                    ",firstpublisheddate as FIRSTPUBLISHEDDATE \n" +
                    ",binding as BINDING \n" +
                    ",manifestation_type as MANIFESTATIONTYPE \n" +
                    ",status as STATUS \n" +
                    ",work_id as WORKID \n" +
                    ",last_pub_date as LASTPUBDATE \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation where u_key in ('%s') order by u_key desc";

    public static String GET_MANIF_REC_DELTA_CURRENT_HIST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",title as TITLE \n" +
                    ",intereditionflag as INTEREDITIONFLAG \n" +
                    ",firstpublisheddate as FIRSTPUBLISHEDDATE \n" +
                    ",binding as BINDING \n" +
                    ",manifestation_type as MANIFESTATIONTYPE \n" +
                    ",status as STATUS \n" +
                    ",work_id as WORKID \n" +
                    ",last_pub_date as LASTPUBDATE \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_manifestation_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_manifestation_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_KEY_MANIF_DELTA_CURRENT_HIST =
            "SELECT u_key as UKEY \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_manifestation_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_manifestation_part)" +
                    " order by rand() limit %s";

    public static String GET_MANIF_REC_CURR_HIST_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",title as TITLE \n" +
                    ",intereditionflag as INTEREDITIONFLAG \n" +
                    ",firstpublisheddate as FIRSTPUBLISHEDDATE \n" +
                    ",binding as BINDING \n" +
                    ",manifestation_type as MANIFESTATIONTYPE \n" +
                    ",status as STATUS \n" +
                    ",work_id as WORKID \n" +
                    ",last_pub_date as LASTPUBDATE \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_part  where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_MANIF_REC_TRANS_FILE_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",title as TITLE \n" +
                    ",intereditionflag as INTEREDITIONFLAG \n" +
                    ",firstpublisheddate as FIRSTPUBLISHEDDATE \n" +
                    ",binding as BINDING \n" +
                    ",manifestation_type as MANIFESTATIONTYPE \n" +
                    ",status as STATUS \n" +
                    ",work_id as WORKID \n" +
                    ",last_pub_date as LASTPUBDATE \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_transform_file_history_part  where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_transform_file_history_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";


    public static String GET_RANDOM_MANIF_IDENT_KEY_CURRENT =
            "SELECT u_key as u_key \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_current_v \n" +
                    " order by rand() limit %s";

    public static String GET_MANIF_IDENT_CURR_REC =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_current_v where u_key in ('%s') order by u_key desc";

    public static String GET_MANIF_IDENT_REC_DELTA_CURRENT =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation_identifier where u_key in ('%s') order by u_key desc";

    public static String GET_MANIF_IDENT_REC_DELTA_CURRENT_HIST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_manifestation_identifier_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_manifestation_identifier_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_KEY_MANIF_IDENT_DELTA_CURRENT_HIST =
            "SELECT u_key as UKEY \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_manifestation_identifier_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_manifestation_identifier_part)" +
                    " order by rand() limit %s";

    public static String GET_MANIF_IDENT_REC_CURR_HIST_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part  where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_MANIF_IDENT_REC_TRANS_FILE_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part  where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_transform_file_history_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_PRODUCT_KEY_CURRENT =
            "SELECT u_key as u_key \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_current_v \n" +
                    " order by rand() limit %s";

    public static String GET_PRODUCT_CURR_REC =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",bindingcode as BINDINGCODE \n" +
                    ",name as NAME \n" +
                    ",shorttitle as SHORTTITLE \n" +
                    ",launchdate as LAUNCHDATE \n" +
                    ",taxcode as TAXCODE \n" +
                    ",status as STATUS \n" +
                    ",manifestationref as MANIFESTATIONREF \n" +
                    ",worksource as WORKSOURCE \n" +
                    ",work_type as WORKTYPE \n" +
                    ",separately_sale_indicator as SEPRATELYSALEINDICATOR \n" +
                    ",trial_allowed_indicator as TRIALALLOWEDINDICATOR \n" +
                    ",f_work_source_ref as FWORKSOURCEREF \n" +
                    ",product_type as PRODUCTTYPE \n" +
                    ",f_revenue_model as REVENUEMODEL \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_current_v where u_key in ('%s') order by u_key desc";

    public static String GET_PRODUCT_REC_DELTA_CURRENT =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",bindingcode as BINDINGCODE \n" +
                    ",name as NAME \n" +
                    ",shorttitle as SHORTTITLE \n" +
                    ",launchdate as LAUNCHDATE \n" +
                    ",taxcode as TAXCODE \n" +
                    ",status as STATUS \n" +
                    ",manifestationref as MANIFESTATIONREF \n" +
                    ",worksource as WORKSOURCE \n" +
                    ",work_type as WORKTYPE \n" +
                    ",separately_sale_indicator as SEPRATELYSALEINDICATOR \n" +
                    ",trial_allowed_indicator as TRIALALLOWEDINDICATOR \n" +
                    ",f_work_source_ref as FWORKSOURCEREF \n" +
                    ",product_type as PRODUCTTYPE \n" +
                    ",f_revenue_model as REVENUEMODEL \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_product where u_key in ('%s') order by u_key desc";

    public static String GET_PRODUCT_REC_DELTA_CURRENT_HIST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",bindingcode as BINDINGCODE \n" +
                    ",name as NAME \n" +
                    ",shorttitle as SHORTTITLE \n" +
                    ",launchdate as LAUNCHDATE \n" +
                    ",taxcode as TAXCODE \n" +
                    ",status as STATUS \n" +
                    ",manifestationref as MANIFESTATIONREF \n" +
                    ",worksource as WORKSOURCE \n" +
                    ",work_type as WORKTYPE \n" +
                    ",separately_sale_indicator as SEPRATELYSALEINDICATOR \n" +
                    ",trial_allowed_indicator as TRIALALLOWEDINDICATOR \n" +
                    ",f_work_source_ref as FWORKSOURCEREF \n" +
                    ",product_type as PRODUCTTYPE \n" +
                    ",f_revenue_model as REVENUEMODEL \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_product_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_product_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_KEY_PRODUCT_DELTA_CURRENT_HIST =
            "SELECT u_key as UKEY \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_product_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_product_part)" +
                    " order by rand() limit %s";


    public static String GET_PRODUCT_REC_CURR_HIST_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",bindingcode as BINDINGCODE \n" +
                    ",name as NAME \n" +
                    ",shorttitle as SHORTTITLE \n" +
                    ",launchdate as LAUNCHDATE \n" +
                    ",taxcode as TAXCODE \n" +
                    ",status as STATUS \n" +
                    ",manifestationref as MANIFESTATIONREF \n" +
                    ",worksource as WORKSOURCE \n" +
                    ",work_type as WORKTYPE \n" +
                    ",separately_sale_indicator as SEPRATELYSALEINDICATOR \n" +
                    ",trial_allowed_indicator as TRIALALLOWEDINDICATOR \n" +
                    ",f_work_source_ref as FWORKSOURCEREF \n" +
                    ",product_type as PRODUCTTYPE \n" +
                    ",f_revenue_model as REVENUEMODEL \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_part  where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_PRODUCT_REC_TRANS_FILE_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",bindingcode as BINDINGCODE \n" +
                    ",name as NAME \n" +
                    ",shorttitle as SHORTTITLE \n" +
                    ",launchdate as LAUNCHDATE \n" +
                    ",taxcode as TAXCODE \n" +
                    ",status as STATUS \n" +
                    ",manifestationref as MANIFESTATIONREF \n" +
                    ",worksource as WORKSOURCE \n" +
                    ",work_type as WORKTYPE \n" +
                    ",separately_sale_indicator as SEPRATELYSALEINDICATOR \n" +
                    ",trial_allowed_indicator as TRIALALLOWEDINDICATOR \n" +
                    ",f_work_source_ref as FWORKSOURCEREF \n" +
                    ",product_type as PRODUCTTYPE \n" +
                    ",f_revenue_model as REVENUEMODEL \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_transform_file_history_part  where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_transform_file_history_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_WORK_PERS_ROLE_KEY_CURRENT =
            "SELECT u_key as u_key \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_current_v \n" +
                    " order by rand() limit %s";

    public static String GET_WORK_PERS_ROLE_CURR_REC =
            "select " +
                    "worksourceref as WORKSOURCEREF \n" +
                    ",personsourceref as PERSONSOURCEREF \n" +
                    ",roletype as ROLETYPE \n" +
                    ",u_key as UKEY \n" +
                    ",sequence as SEQUENCE \n" +
                    ",deduplicator as DEDUPLICATOR \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_current_v where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_PERS_ROLE_REC_DELTA_CURRENT =
            "select " +
                    "worksourceref as WORKSOURCEREF \n" +
                    ",personsourceref as PERSONSOURCEREF \n" +
                    ",roletype as ROLETYPE \n" +
                    ",u_key as UKEY \n" +
                    ",sequence as SEQUENCE \n" +
                    ",deduplicator as DEDUPLICATOR \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_person_role where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_PERS_ROLE_REC_DELTA_CURRENT_HIST =
            "select " +
                    "worksourceref as WORKSOURCEREF \n" +
                    ",personsourceref as PERSONSOURCEREF \n" +
                    ",roletype as ROLETYPE \n" +
                    ",u_key as UKEY \n" +
                    ",sequence as SEQUENCE \n" +
                    ",deduplicator as DEDUPLICATOR \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_person_role_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_person_role_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_KEY_WORK_PERS_ROLE_DELTA_CURRENT_HIST =
            "SELECT u_key as UKEY \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_person_role_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_person_role_part)" +
                    " order by rand() limit %s";

    public static String GET_WORK_PERS_ROLE_REC_CURR_HIST_DATA =
            "select " +
                    "worksourceref as WORKSOURCEREF \n" +
                    ",personsourceref as PERSONSOURCEREF \n" +
                    ",roletype as ROLETYPE \n" +
                    ",u_key as UKEY \n" +
                    ",sequence as SEQUENCE \n" +
                    ",deduplicator as DEDUPLICATOR \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_part  where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_WORK_PERS_ROLE_REC_TRANS_FILE_DATA =
            "select " +
                    "worksourceref as WORKSOURCEREF \n" +
                    ",personsourceref as PERSONSOURCEREF \n" +
                    ",roletype as ROLETYPE \n" +
                    ",u_key as UKEY \n" +
                    ",sequence as SEQUENCE \n" +
                    ",deduplicator as DEDUPLICATOR \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_transform_file_history_part  where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_transform_file_history_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_WORK_RELATION_KEY_CURRENT =
            "SELECT u_key as u_key \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_current_v \n" +
                    " order by rand() limit %s";

    public static String GET_WORK_RELATION_CURR_REC =
            "select " +
                    "parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_current_v where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_RELATION_CURR_DATA =
            "select " +
                    " u_key as UKEY \n" +
                    ",parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_current_v where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_RELATION_REC_DELTA_CURRENT =
            "select " +
                    "parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_relationship where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_RELATION_REC_DELTA_CURRENT_HIST =
            "select " +
                    "parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_relationship_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_relationship_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_KEY_WORK_RELATION_DELTA_CURRENT_HIST =
            "SELECT u_key as UKEY \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_relationship_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_relationship_part)" +
                    " order by rand() limit %s";

    public static String GET_WORK_RELATION_REC_CURR_HIST_DATA =
            "select " +
                    "parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_part  where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_WORK_RELATION_REC_TRANS_FILE_DATA =
            "select " +
                    "parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_transform_file_history_part  where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_transform_file_history_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";


    public static String GET_RANDOM_WORK_KEY_CURRENT =
            "SELECT u_key as u_key \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_current_v \n" +
                    " order by rand() limit %s";

    public static String GET_WORK_CURR_REC =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",subtitle as SUBTITLE \n" +
                    ",u_key as UKEY \n" +
                    ",volumeno as VOLUMENO \n" +
                    ",copyrightyear as COPYRIGHTYEAR \n" +
                    ",editionno as EDITIONNO \n" +
                    ",pmc as PMC \n" +
                    ",work_type as WORKTYPE \n" +
                    ",statuscode as STATUSCODE \n" +
                    ",imprintcode as IMPRINTCODE \n" +
                    ",te_opco as TEOPCO \n" +
                    ",opco as OPCO \n" +
                    ",resp_centre as RESPCENTRE \n" +
                    ",pmg as PMG \n" +
                    ",languagecode as LANGUAGECODE \n" +
                    ",electro_rights_indicator as ELECTRORIGHTSINDICATOR \n" +
                    ",f_oa_journal_type as FOAJOURNALTYPE \n" +
                    ",f_society_ownership as FSOCIETYOWNERSHIP \n" +
                    ",subscription_type as SUBSCRIPTIONTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_current_v where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_CURR_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",subtitle as SUBTITLE \n" +
                    ",volumeno as VOLUMENO \n" +
                    ",copyrightyear as COPYRIGHTYEAR \n" +
                    ",editionno as EDITIONNO \n" +
                    ",pmc as PMC \n" +
                    ",work_type as WORKTYPE \n" +
                    ",statuscode as STATUSCODE \n" +
                    ",imprintcode as IMPRINTCODE \n" +
                    ",te_opco as TEOPCO \n" +
                    ",opco as OPCO \n" +
                    ",resp_centre as RESPCENTRE \n" +
                    ",pmg as PMG \n" +
                    ",languagecode as LANGUAGECODE \n" +
                    ",electro_rights_indicator as ELECTRORIGHTSINDICATOR \n" +
                    ",f_oa_journal_type as FOAJOURNALTYPE \n" +
                    ",f_society_ownership as FSOCIETYOWNERSHIP \n" +
                    ",subscription_type as SUBSCRIPTIONTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_current_v where sourceref in ('%s') order by sourceref desc";


    public static String GET_WORK_REC_DIFF_DELTA_CURRENT =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",subtitle as SUBTITLE \n" +
                    ",u_key as UKEY \n" +
                    ",volumeno as VOLUMENO \n" +
                    ",copyrightyear as COPYRIGHTYEAR \n" +
                    ",editionno as EDITIONNO \n" +
                    ",pmc as PMC \n" +
                    ",work_type as WORKTYPE \n" +
                    ",statuscode as STATUSCODE \n" +
                    ",imprintcode as IMPRINTCODE \n" +
                    ",te_opco as TEOPCO \n" +
                    ",opco as OPCO \n" +
                    ",resp_centre as RESPCENTRE \n" +
                    ",pmg as PMG \n" +
                    ",languagecode as LANGUAGECODE \n" +
                    ",electro_rights_indicator as ELECTRORIGHTSINDICATOR \n" +
                    ",f_oa_journal_type as FOAJOURNALTYPE \n" +
                    ",f_society_ownership as FSOCIETYOWNERSHIP \n" +
                    ",subscription_type as SUBSCRIPTIONTYPE \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_REC_DIFF_DELTA_CURRENT_HIST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",subtitle as SUBTITLE \n" +
                    ",u_key as UKEY \n" +
                    ",volumeno as VOLUMENO \n" +
                    ",copyrightyear as COPYRIGHTYEAR \n" +
                    ",editionno as EDITIONNO \n" +
                    ",pmc as PMC \n" +
                    ",work_type as WORKTYPE \n" +
                    ",statuscode as STATUSCODE \n" +
                    ",imprintcode as IMPRINTCODE \n" +
                    ",te_opco as TEOPCO \n" +
                    ",opco as OPCO \n" +
                    ",resp_centre as RESPCENTRE \n" +
                    ",pmg as PMG \n" +
                    ",languagecode as LANGUAGECODE \n" +
                    ",electro_rights_indicator as ELECTRORIGHTSINDICATOR \n" +
                    ",f_oa_journal_type as FOAJOURNALTYPE \n" +
                    ",f_society_ownership as FSOCIETYOWNERSHIP \n" +
                    ",subscription_type as SUBSCRIPTIONTYPE \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_KEY_WORK_DELTA_CURRENT_HIST =
            "SELECT u_key as UKEY \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_part)" +
                    " order by rand() limit %s";

    public static String GET_WORK_REC_CURR_HIST_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",subtitle as SUBTITLE \n" +
                    ",u_key as UKEY \n" +
                    ",volumeno as VOLUMENO \n" +
                    ",copyrightyear as COPYRIGHTYEAR \n" +
                    ",editionno as EDITIONNO \n" +
                    ",pmc as PMC \n" +
                    ",work_type as WORKTYPE \n" +
                    ",statuscode as STATUSCODE \n" +
                    ",imprintcode as IMPRINTCODE \n" +
                    ",te_opco as TEOPCO \n" +
                    ",opco as OPCO \n" +
                    ",resp_centre as RESPCENTRE \n" +
                    ",pmg as PMG \n" +
                    ",languagecode as LANGUAGECODE \n" +
                    ",electro_rights_indicator as ELECTRORIGHTSINDICATOR \n" +
                    ",f_oa_journal_type as FOAJOURNALTYPE \n" +
                    ",f_society_ownership as FOAJOURNALTYPE \n" +
                    ",subscription_type as SUBSCRIPTIONTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_part  where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";


    public static String GET_WORK_REC_TRANS_FILE_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",subtitle as SUBTITLE \n" +
                    ",u_key as UKEY \n" +
                    ",volumeno as VOLUMENO \n" +
                    ",copyrightyear as COPYRIGHTYEAR \n" +
                    ",editionno as EDITIONNO \n" +
                    ",pmc as PMC \n" +
                    ",work_type as WORKTYPE \n" +
                    ",statuscode as STATUSCODE \n" +
                    ",imprintcode as IMPRINTCODE \n" +
                    ",te_opco as TEOPCO \n" +
                    ",opco as OPCO \n" +
                    ",resp_centre as RESPCENTRE \n" +
                    ",pmg as PMG \n" +
                    ",languagecode as LANGUAGECODE \n" +
                    ",electro_rights_indicator as ELECTRORIGHTSINDICATOR \n" +
                    ",f_oa_journal_type as FOAJOURNALTYPE \n" +
                    ",f_society_ownership as FOAJOURNALTYPE \n" +
                    ",subscription_type as SUBSCRIPTIONTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_transform_file_history_part  where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_transform_file_history_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";


    public static String GET_RANDOM_WORK_IDENT_KEY_CURRENT =
            "SELECT u_key as u_key \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_current_v \n" +
                    " order by rand() limit %s";

    public static String GET_WORK_IDENT_CURR_REC =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_current_v where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_IDENT_REC_DELTA_CURRENT =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_identifier where u_key in ('%s') order by u_key desc";


    public static String GET_WORK_IDENT_REC_DELTA_CURRENT_HIST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    ",delta_mode as DELTA_MODE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_identifier_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_identifier_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_KEY_WORK_IDENT_DELTA_CURRENT_HIST =
            "SELECT u_key as UKEY \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_identifier_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_work_identifier_part)" +
                    " order by rand() limit %s";

    public static String GET_WORK_IDENT_REC_CURR_HIST_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_part  where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_WORK_IDENT_REC_TRANS_FILE_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_transform_file_history_part  where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_transform_file_history_part)" +
                    "and u_key in ('%s') \n" +
                    "order by u_key desc";


    public static String GET_RANDOM_PERSON_KEY_CURRENT =
            "SELECT u_key as u_key \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_current_v \n" +
                    " order by rand() limit %s";

    public static String GET_PERSON_CURR_REC =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",u_key as UKEY" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQ_ERR " +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_current_v where u_key in \n" +
                    "(%s) order by u_key desc";

    public static String GET_PERSON_REC_CURR_HIST_DATA =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",u_key as UKEY" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQ_ERR " +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_part  where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_part)" +
                    "and u_key in (%s) \n" +
                    "order by u_key desc";

    public static String GET_PERSON_REC_TRANS_FILE_DATA =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",u_key as UKEY" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQ_ERR " +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_transform_file_history_part  where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_transform_file_history_part)" +
                    "and u_key in (%s) \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_PERSON_KEY_DIFF_TRANS_FILE =
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
                    "            coalesce (crr.dq_err, 'na') <> coalesce (prev.dq_err, 'na'))order by rand() limit %s";

    public static String GET_PERSON_REC_DIFF_TRANS_FILE =
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
                    "select " +
                    "sourceref as SOURCEREF" +
                    ",u_key as UKEY" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQ_ERR " +
                    ",delta_mode as DELTA_MODE from(" +
                    "    select crr.sourceref, crr.u_key, crr.firstname, crr.familyname, crr.peoplehub_id, crr.email_address, crr.dq_err,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.sourceref, prev.u_key, prev.firstname, prev.familyname, prev.peoplehub_id, prev.email_address, prev.dq_err,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.sourceref, crr.u_key, crr.firstname, crr.familyname, crr.peoplehub_id, crr.email_address, crr.dq_err,'C' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 0) <> coalesce(prev.sourceref,0 ) or\n" +
                    "            coalesce(crr.u_key, 0) <> coalesce(prev.u_key, 0) or\n" +
                    "            coalesce (crr.firstname, 'na') <> coalesce (prev.firstname, 'na') or\n" +
                    "            coalesce (crr.familyname, 'na') <> coalesce (prev.familyname, 'na') or\n" +
                    "            coalesce (crr.peoplehub_id, 'na') <> coalesce (prev.peoplehub_id, 'na') or\n" +
                    "            coalesce (crr.email_address, 'na') <> coalesce (prev.email_address, 'na') or\n" +
                    "            coalesce (crr.dq_err, 'na') <> coalesce (prev.dq_err, 'na')))where u_key in (%s) order by u_key desc ";


    public static String GET_RANDOM_ACCPROD_KEY_DIFF_TRANS_FILE =
            "with crr_dataset as(\n" +
                    "  select sourceref, accountableproduct, accountablename, accountableparent, u_key, dq_err\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select sourceref, accountableproduct, accountablename, accountableparent, u_key, dq_err\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part\n" +
                    "  where transform_file_ts = (select distinct transform_file_ts\n" +
                    "                                    from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn   \n" +
                    "                                            from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part dhap\n" +
                    "                                            )\n" +
                    "                                    where rn = 2\n" +
                    "                                    )\n" +
                    "                                    )                                  \n" +
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
                    "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref, 'null') or\n" +
                    "            coalesce(crr.accountableproduct, 'null') <> coalesce(prev.accountableproduct, 'null') or\n" +
                    "            coalesce (crr.accountablename, 'null') <> coalesce (prev.accountablename, 'null') or\n" +
                    "            coalesce (crr.accountableparent, 'null') <> coalesce (prev.accountableparent, 'null') or\n" +
                    "            coalesce (crr.u_key, 'null') <> coalesce (prev.u_key, 'null') or\n" +
                    "            coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null'))order by rand() limit %s";

    public static String GET_ACCPROD_REC_DIFF_TRANS_FILE =
            "with crr_dataset as(\n" +
                    "  select sourceref, accountableproduct, accountablename, accountableparent, u_key, dq_err\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select sourceref, accountableproduct, accountablename, accountableparent, u_key, dq_err\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part\n" +
                    "  where transform_file_ts = (select distinct transform_file_ts\n" +
                    "                                    from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn   \n" +
                    "                                            from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_transform_file_history_part dhap\n" +
                    "                                            )\n" +
                    "                                    where rn = 2\n" +
                    "                                    )\n" +
                    "                                    ) \n" +
                    "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE from ("+
                    "    select crr.u_key, crr.sourceref,crr.accountableproduct,crr.accountablename,crr.accountableparent,crr.dq_err,'I' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key,prev.sourceref,prev.accountableproduct,prev.accountablename,prev.accountableparent,prev.dq_err, 'D' as delta_mode \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key, crr.sourceref,crr.accountableproduct,crr.accountablename,crr.accountableparent,crr.dq_err,'C' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref, 'null') or\n" +
                    "            coalesce(crr.accountableproduct, 'null') <> coalesce(prev.accountableproduct, 'null') or\n" +
                    "            coalesce (crr.accountablename, 'null') <> coalesce (prev.accountablename, 'null') or\n" +
                    "            coalesce (crr.accountableparent, 'null') <> coalesce (prev.accountableparent, 'null') or\n" +
                    "            coalesce (crr.u_key, 'null') <> coalesce (prev.u_key, 'null') or\n" +
                    "            coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))where u_key in ('%s') order by u_key desc ";



    public static String GET_RANDOM_WORK_KEY_DIFF_TRANS_FILE =
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
                    " coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date))order by rand() limit %s";

    public static String GET_WORK_REC_DIFF_TRANS_FILE =
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
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_transform_file_history_part dhap)where rn = 2)) \n" +
                    "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",subtitle as SUBTITLE \n" +
                    ",u_key as UKEY \n" +
                    ",volumeno as VOLUMENO \n" +
                    ",copyrightyear as COPYRIGHTYEAR \n" +
                    ",editionno as EDITIONNO \n" +
                    ",pmc as PMC \n" +
                    ",work_type as WORKTYPE \n" +
                    ",statuscode as STATUSCODE \n" +
                    ",imprintcode as IMPRINTCODE \n" +
                    ",te_opco as TEOPCO \n" +
                    ",opco as OPCO \n" +
                    ",resp_centre as RESPCENTRE \n" +
                    ",pmg as PMG \n" +
                    ",languagecode as LANGUAGECODE \n" +
                    ",electro_rights_indicator as ELECTRORIGHTSINDICATOR \n" +
                    ",f_oa_journal_type as FOAJOURNALTYPE \n" +
                    ",f_society_ownership as FSOCIETYOWNERSHIP \n" +
                    ",subscription_type as SUBSCRIPTIONTYPE \n" +
                    ",delta_mode as DELTA_MODE from ("+
                    "    select crr.u_key,crr.sourceref,crr.title,crr.subtitle,crr.volumeno,crr.copyrightyear,crr.editionno,crr.pmc,crr.work_type,crr.statuscode \n" +
                    " ,crr.imprintcode,crr.te_opco,crr.opco,crr.resp_centre,crr.pmg,crr.languagecode,crr.electro_rights_indicator,crr.f_oa_journal_type,crr.f_society_ownership \n" +
                    ",crr.subscription_type,'I' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.u_key,prev.sourceref,prev.title,prev.subtitle,prev.volumeno,prev.copyrightyear,prev.editionno,prev.pmc,prev.work_type,prev.statuscode \n" +
                    " ,prev.imprintcode,prev.te_opco,prev.opco,prev.resp_centre,prev.pmg,prev.languagecode,prev.electro_rights_indicator,prev.f_oa_journal_type,prev.f_society_ownership \n" +
                    ",prev.subscription_type,'D' as delta_mode \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key,crr.sourceref,crr.title,crr.subtitle,crr.volumeno,crr.copyrightyear,crr.editionno,crr.pmc,crr.work_type,crr.statuscode \n" +
                    " ,crr.imprintcode,crr.te_opco,crr.opco,crr.resp_centre,crr.pmg,crr.languagecode,crr.electro_rights_indicator,crr.f_oa_journal_type,crr.f_society_ownership \n" +
                    ",crr.subscription_type,'C' as delta_mode \n" +
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
                    " coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date)))where u_key in ('%s') order by u_key desc ";

    public static String GET_RANDOM_MANIF_KEY_DIFF_TRANS_FILE =
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
                    " coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null'))order by rand() limit %s";

    public static String GET_MANIF_REC_DIFF_TRANS_FILE =
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
                    "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",title as TITLE \n" +
                    ",intereditionflag as INTEREDITIONFLAG \n" +
                    ",firstpublisheddate as FIRSTPUBLISHEDDATE \n" +
                    ",binding as BINDING \n" +
                    ",manifestation_type as MANIFESTATIONTYPE \n" +
                    ",status as STATUS \n" +
                    ",work_id as WORKID \n" +
                    ",last_pub_date as LASTPUBDATE \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE from ("+
                    "    select crr.u_key,crr.sourceref,crr.title,crr.intereditionflag,crr.firstpublisheddate,crr.binding,crr.manifestation_type,crr.status,crr.work_id,crr.last_pub_date,crr.dq_err,'I' as delta_mode \n"+
                    "    from crr_dataset crr\n"+
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n"+
                    "    where prev.u_key is null\n"+
                    "    union\n"+
                    "    select prev.u_key,prev.sourceref,prev.title,prev.intereditionflag,prev.firstpublisheddate,prev.binding,prev.manifestation_type,prev.status,prev.work_id,prev.last_pub_date,prev.dq_err,'D' as delta_mode \n"+
                    "    from prev_dataset prev\n"+
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n"+
                    "    where crr.u_key is null\n"+
                    "    union\n"+
                    "    select crr.u_key,crr.sourceref,crr.title,crr.intereditionflag,crr.firstpublisheddate,crr.binding,crr.manifestation_type,crr.status,crr.work_id,crr.last_pub_date,crr.dq_err,'C' as delta_mode \n"+
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
                    " coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))where u_key in ('%s') order by u_key desc ";


    public static String GET_RANDOM_MANIF_IDENT_KEY_DIFF_TRANS_FILE =
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
                    "            coalesce (crr.identifier_type, 'null') <> coalesce (prev.identifier_type, 'null'))order by rand() limit %s";

    public static String GET_MANIF_IDENT_REC_DIFF_TRANS_FILE =
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
                    "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    ",delta_mode as DELTA_MODE from( \n" +
                    "    select crr.u_key, crr.sourceref,crr.identifier,crr.identifier_type,'I' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.u_key, prev.sourceref,prev.identifier,prev.identifier_type,'D' as delta_mode \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key, crr.sourceref,crr.identifier,crr.identifier_type,'C' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref,'null' ) or\n" +
                    "            coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
                    "            coalesce (crr.identifier, 'null') <> coalesce (prev.identifier, 'null') or\n" +
                    "            coalesce (crr.identifier_type, 'null') <> coalesce (prev.identifier_type, 'null')))where u_key in ('%s') order by u_key desc ";

    public static String GET_RANDOM_PRODUCT_KEY_DIFF_TRANS_FILE =
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
                    "    coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null'))order by rand() limit %s";

    public static String GET_PRODUCT_DIFF_TRANS_FILE =
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
                    "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",bindingcode as BINDINGCODE \n" +
                    ",name as NAME \n" +
                    ",shorttitle as SHORTTITLE \n" +
                    ",launchdate as LAUNCHDATE \n" +
                    ",taxcode as TAXCODE \n" +
                    ",status as STATUS \n" +
                    ",manifestationref as MANIFESTATIONREF \n" +
                    ",worksource as WORKSOURCE \n" +
                    ",work_type as WORKTYPE \n" +
                    ",separately_sale_indicator as SEPRATELYSALEINDICATOR \n" +
                    ",trial_allowed_indicator as TRIALALLOWEDINDICATOR \n" +
                    ",f_work_source_ref as FWORKSOURCEREF \n" +
                    ",product_type as PRODUCTTYPE \n" +
                    ",f_revenue_model as REVENUEMODEL \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE from(\n" +
                    "    select crr.sourceref,crr.bindingcode,crr.u_key,crr.name,crr.shorttitle,crr.launchdate,crr.taxcode,crr.status,crr.manifestationref,crr.worksource,crr.work_type,crr.separately_sale_indicator,crr.trial_allowed_indicator,crr.f_work_source_ref,crr.product_type,crr.f_revenue_model,crr.dq_err,'I' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "    left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.sourceref,prev.bindingcode,prev.u_key,prev.name,prev.shorttitle,prev.launchdate,prev.taxcode,prev.status,prev.manifestationref,prev.worksource,prev.work_type,prev.separately_sale_indicator,prev.trial_allowed_indicator,prev.f_work_source_ref,prev.product_type,prev.f_revenue_model,prev.dq_err,'D' as delta_mode \n" +
                    "    from prev_dataset prev\n" +
                    "    left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select prev.sourceref,prev.bindingcode,prev.u_key,prev.name,prev.shorttitle,prev.launchdate,prev.taxcode,prev.status,prev.manifestationref,prev.worksource,prev.work_type,prev.separately_sale_indicator,prev.trial_allowed_indicator,prev.f_work_source_ref,prev.product_type,prev.f_revenue_model,prev.dq_err,'C' as delta_mode \n" +
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
                    "    coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))where u_key in ('%s') order by u_key desc ";

    public static String GET_RANDOM_WORK_RELATION_KEY_DIFF_TRANS_FILE =
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
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_transform_file_history_part dhap)where rn = 2)) \n" +
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
                    " coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null'))order by rand() limit %s";

    public static String GET_WORK_RELATION_REC_DIFF_TRANS_FILE =
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
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_relationship_transform_file_history_part dhap)where rn = 2)) \n" +
                    "select " +
                    "parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE from(\n" +
                    "    select crr.u_key,crr.parentref, crr.childref, crr.relationtyperef, crr.modifiedon, crr.dq_err, 'I' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.u_key,prev.parentref, prev.childref, prev.relationtyperef, prev.modifiedon, prev.dq_err, 'D' as delta_mode \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key,crr.parentref, crr.childref, crr.relationtyperef, crr.modifiedon, crr.dq_err, 'C' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.u_key, 'null') <> coalesce(prev.u_key,'null' ) or\n" +
                    " coalesce(crr.parentref, 'null') <> coalesce(prev.parentref, 'null') or\n" +
                    " coalesce (crr.childref, 'null') <> coalesce (prev.childref, 'null') or\n" +
                    " coalesce (crr.relationtyperef, 'null') <> coalesce (prev.relationtyperef, 'null') or\n" +
                    " coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    " coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))where u_key in ('%s') order by u_key desc ";

    public static String GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_TRANS_FILE =
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
                    " coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null'))order by rand() limit %s";

    public static String GET_WORK_PERS_ROLE_REC_DIFF_TRANS_FILE =
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
                    "select " +
                    "worksourceref as WORKSOURCEREF \n" +
                    ",personsourceref as PERSONSOURCEREF \n" +
                    ",roletype as ROLETYPE \n" +
                    ",u_key as UKEY \n" +
                    ",sequence as SEQUENCE \n" +
                    ",deduplicator as DEDUPLICATOR \n" +
                    ",dq_err as DQ_ERR \n" +
                    ",delta_mode as DELTA_MODE from( \n" +
                    "    select crr.worksourceref,crr.personsourceref, crr.roletype, crr.u_key, crr.sequence, crr.deduplicator,crr.modifiedon,crr.dq_err,'I' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.worksourceref,prev.personsourceref, prev.roletype, prev.u_key, prev.sequence, prev.deduplicator,prev.modifiedon,prev.dq_err,'D' as delta_mode \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.worksourceref,crr.personsourceref, crr.roletype, crr.u_key, crr.sequence, crr.deduplicator,crr.modifiedon,crr.dq_err,'C' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.worksourceref, 'null') <> coalesce(prev.worksourceref,'null' ) or\n" +
                    "            coalesce(crr.personsourceref, 'null') <> coalesce(prev.personsourceref, 'null') or\n" +
                    "            coalesce (crr.roletype, 'null') <> coalesce (prev.roletype, 'null') or\n" +
                    "            coalesce (crr.u_key, 'null') <> coalesce (prev.u_key, 'null') or\n" +
                    " coalesce (crr.sequence, 'null') <> coalesce (prev.sequence, 'null') or\n" +
                    " coalesce (crr.deduplicator, 'null') <> coalesce (prev.deduplicator, 'null') or\n" +
                    " coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    " coalesce (crr.dq_err, 'null') <> coalesce (prev.dq_err, 'null')))where u_key in ('%s') order by u_key desc ";


    public static String GET_RANDOM_WORK_IDENT_KEY_DIFF_TRANS_FILE =
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
                    "            coalesce (crr.identifier_type, 'null') <> coalesce (prev.identifier_type, 'null'))order by rand() limit %s";


    public static String GET_WORK_IDENT_REC_DIFF_TRANS_FILE =
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
                    "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    ",delta_mode as DELTA_MODE from( \n" +
                    "    select crr.u_key, crr.sourceref,crr.identifier,crr.identifier_type,'I' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.u_key, prev.sourceref,prev.identifier,prev.identifier_type,'D' as delta_mode \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key, crr.sourceref,crr.identifier,crr.identifier_type,'C' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.sourceref, 'null') <> coalesce(prev.sourceref,'null' ) or\n" +
                    "            coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
                    "            coalesce (crr.identifier, 'null') <> coalesce (prev.identifier, 'null') or\n" +
                    "            coalesce (crr.identifier_type, 'null') <> coalesce (prev.identifier_type, 'null')))where u_key in ('%s') order by u_key desc ";

    public static String GET_PERSON_REC_DELTA_CURR =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",u_key as UKEY" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQ_ERR " +
                    ",delta_mode as DELTA_MODE "+
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_person where u_key in \n" +
                    "(%s) order by u_key desc";

    public static String GET_PERSON_REC_DELTA_CURR_HIST =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",u_key as UKEY" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQ_ERR " +
                    ",delta_mode as DELTA_MODE "+
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_person_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_person_part)" +
                    "and u_key in (%s) \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_KEY_PERS_DELTA_CURRENT_HIST =
            "SELECT u_key as UKEY \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_person_part  where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_person_part)" +
                    " order by rand() limit %s";


    public static String GET_RANDOM_ACCPROD_KEY_DIFF_DELTACURR_CURRHIST =
            "select u_key as UKEY from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_accountable_product d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_part c ))\n" +
                    " order by rand() limit %s";

    public static String GET_RANDOM_ACCPROD_KEY_DIFF_DELTACURR_EXCL =
            "select u_key as UKEY from \n" +
                    "(select c.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_excl_delta as c union all \n" +
                    "select d.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_accountable_product as d) \n" +
                    " order by rand() limit %s";

    public static String GET_ACCPROD_REC_DIFF_DELTACURR_EXCL =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR from \n" +
                    "(select c.u_key,c.sourceref,c.accountableproduct,c.accountablename,c.accountableparent,c.dq_err \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_excl_delta as c union all \n" +
                    "select d.u_key,d.sourceref,d.accountableproduct,d.accountablename,d.accountableparent,d.dq_err \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_accountable_product as d) \n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_ACCPROD_REC_LATEST =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_latest \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_MANIF_KEY_DIFF_DELTACURR_EXCL =
            "select u_key as UKEY from \n" +
                    "(select c.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_excl_delta as c union all \n" +
                    "select d.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation as d) \n" +
                    " order by rand() limit %s";

    public static String GET_MANIF_REC_DIFF_DELTACURR_EXCL =
            "select sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",title as TITLE \n" +
                    ",intereditionflag as INTEREDITIONFLAG \n" +
                    ",firstpublisheddate as FIRSTPUBLISHEDDATE \n" +
                    ",binding as BINDING \n" +
                    ",manifestation_type as MANIFESTATIONTYPE \n" +
                    ",status as STATUS \n" +
                    ",work_id as WORKID \n" +
                    ",last_pub_date as LASTPUBDATE \n" +
                    ",dq_err as DQ_ERR from \n" +
                    "(select c.u_key,c.sourceref,c.title,c.intereditionflag,c.firstpublisheddate,c.binding,c.manifestation_type,c.status,c.work_id,c.last_pub_date,c.dq_err from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_excl_delta as c union all \n" +
                    "select d.u_key,d.sourceref,d.title,d.intereditionflag,d.firstpublisheddate,d.binding,d.manifestation_type,d.status,d.work_id,d.last_pub_date,d.dq_err \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation as d) \n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_MANIF_REC_LATEST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",title as TITLE \n" +
                    ",intereditionflag as INTEREDITIONFLAG \n" +
                    ",firstpublisheddate as FIRSTPUBLISHEDDATE \n" +
                    ",binding as BINDING \n" +
                    ",manifestation_type as MANIFESTATIONTYPE \n" +
                    ",status as STATUS \n" +
                    ",work_id as WORKID \n" +
                    ",last_pub_date as LASTPUBDATE \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_latest where u_key in ('%s') order by u_key desc";

    public static String GET_MANIF_IDENT_REC_LATEST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_latest where u_key in ('%s') order by u_key desc";

    public static String GET_PRODUCT_REC_LATEST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",bindingcode as BINDINGCODE \n" +
                    ",name as NAME \n" +
                    ",shorttitle as SHORTTITLE \n" +
                    ",launchdate as LAUNCHDATE \n" +
                    ",taxcode as TAXCODE \n" +
                    ",status as STATUS \n" +
                    ",manifestationref as MANIFESTATIONREF \n" +
                    ",worksource as WORKSOURCE \n" +
                    ",work_type as WORKTYPE \n" +
                    ",separately_sale_indicator as SEPRATELYSALEINDICATOR \n" +
                    ",trial_allowed_indicator as TRIALALLOWEDINDICATOR \n" +
                    ",f_work_source_ref as FWORKSOURCEREF \n" +
                    ",product_type as PRODUCTTYPE \n" +
                    ",f_revenue_model as REVENUEMODEL \n" +
                    ",dq_err as DQ_ERR  \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_latest \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_WORK_PERS_ROLE_REC_LATEST =
            "select " +
                    "worksourceref as WORKSOURCEREF \n" +
                    ",personsourceref as PERSONSOURCEREF \n" +
                    ",roletype as ROLETYPE \n" +
                    ",u_key as UKEY \n" +
                    ",sequence as SEQUENCE \n" +
                    ",deduplicator as DEDUPLICATOR \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_latest \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_WORK_RELATION_REC_LATEST =
            "select " +
                    "parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_latest \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_WORK_REC_LATEST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",subtitle as SUBTITLE \n" +
                    ",u_key as UKEY \n" +
                    ",volumeno as VOLUMENO \n" +
                    ",copyrightyear as COPYRIGHTYEAR \n" +
                    ",editionno as EDITIONNO \n" +
                    ",pmc as PMC \n" +
                    ",work_type as WORKTYPE \n" +
                    ",statuscode as STATUSCODE \n" +
                    ",imprintcode as IMPRINTCODE \n" +
                    ",te_opco as TEOPCO \n" +
                    ",opco as OPCO \n" +
                    ",resp_centre as RESPCENTRE \n" +
                    ",pmg as PMG \n" +
                    ",languagecode as LANGUAGECODE \n" +
                    ",electro_rights_indicator as ELECTRORIGHTSINDICATOR \n" +
                    ",f_oa_journal_type as FOAJOURNALTYPE \n" +
                    ",f_society_ownership as FSOCIETYOWNERSHIP \n" +
                    ",subscription_type as SUBSCRIPTIONTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_latest \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_WORK_IDENT_REC_LATEST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_latest \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";


    public static String GET_MANIF_IDENT_REC_DIFF_DELTACURR_EXCL =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE from \n" +
                    "(select c.u_key,c.sourceref,c.identifier,c.identifier_type from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_excl_delta as c union all \n" +
                    "select d.u_key,d.sourceref,d.identifier,d.identifier_type \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation_identifier as d) \n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_PRODUCT_DIFF_DELTACURR_EXCL =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",bindingcode as BINDINGCODE \n" +
                    ",name as NAME \n" +
                    ",shorttitle as SHORTTITLE \n" +
                    ",launchdate as LAUNCHDATE \n" +
                    ",taxcode as TAXCODE \n" +
                    ",status as STATUS \n" +
                    ",manifestationref as MANIFESTATIONREF \n" +
                    ",worksource as WORKSOURCE \n" +
                    ",work_type as WORKTYPE \n" +
                    ",separately_sale_indicator as SEPRATELYSALEINDICATOR \n" +
                    ",trial_allowed_indicator as TRIALALLOWEDINDICATOR \n" +
                    ",f_work_source_ref as FWORKSOURCEREF \n" +
                    ",product_type as PRODUCTTYPE \n" +
                    ",f_revenue_model as REVENUEMODEL \n" +
                    ",dq_err as DQ_ERR from \n" +
                    "(select c.sourceref,c.bindingcode,c.u_key,c.name,c.shorttitle,c.launchdate,c.taxcode,c.status,c.manifestationref,\n" +
                    "c.worksource,c.work_type,c.separately_sale_indicator,c.trial_allowed_indicator,c.f_work_source_ref,\n" +
                    "c.product_type,c.f_revenue_model,c.dq_err  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_excl_delta as c union all \n" +
                    "select d.sourceref,d.bindingcode,d.u_key,d.name,d.shorttitle,d.launchdate,d.taxcode,d.status,d.manifestationref, \n" +
                    "d.worksource,d.work_type,d.separately_sale_indicator,d.trial_allowed_indicator,d.f_work_source_ref,d.product_type,d.f_revenue_model,d.dq_err \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_product as d) \n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_RANDOM_MANIF_IDENT_KEY_DIFF_DELTACURR_EXCL =
            "select u_key as UKEY from \n" +
                    "(select c.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_excl_delta as c union all \n" +
                    "select d.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation_identifier as d) \n" +
                    " order by rand() limit %s";

    public static String GET_RANDOM_PRODUCT_KEY_DIFF_DELTACURR_EXCL =
            "select u_key as UKEY from \n" +
                    "(select c.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_excl_delta as c union all \n" +
                    "select d.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_product as d) \n" +
                    " order by rand() limit %s";

    public static String GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_DELTACURR_EXCL =
            "select u_key as UKEY from \n" +
                    "(select c.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_excl_delta as c union all \n" +
                    "select d.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_person_role as d) \n" +
                    " order by rand() limit %s";

    public static String GET_WORK_PERS_ROLE_REC_DIFF_DELTACURR_EXCL =
            "select " +
                    "worksourceref as WORKSOURCEREF \n" +
                    ",personsourceref as PERSONSOURCEREF \n" +
                    ",roletype as ROLETYPE \n" +
                    ",u_key as UKEY \n" +
                    ",sequence as SEQUENCE \n" +
                    ",deduplicator as DEDUPLICATOR \n" +
                    ",dq_err as DQ_ERR from \n" +
                    "(select c.worksourceref,c.personsourceref,c.roletype,c.u_key,c.sequence,c.deduplicator,c.modifiedon,c.dq_err from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_excl_delta as c union all \n" +
                    "select d.worksourceref,d.personsourceref,d.roletype,d.u_key,d.sequence,d.deduplicator,d.modifiedon,d.dq_err \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_person_role as d) \n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_RANDOM_WORK_RELATION_KEY_DIFF_DELTACURR_EXCL =
            "select u_key as UKEY from \n" +
                    "(select c.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_excl_delta as c union all \n" +
                    "select d.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_relationship as d) \n" +
                    " order by rand() limit %s";

    public static String GET_WORK_RELATION_REC_DIFF_DELTACURR_EXCL =
            "select " +
                    "parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR from\n" +
                    "(select c.u_key,c.parentref,c.childref,c.relationtyperef,c.modifiedon,c.dq_err from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_excl_delta as c union all \n" +
                     "select d.u_key,d.parentref,d.childref,d.relationtyperef,d.modifiedon,d.dq_err \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_relationship as d) \n" +
            " where u_key in ('%s') order by u_key desc";

    public static String GET_RANDOM_WORK_KEY_DIFF_DELTACURR_EXCL =
            "select u_key as UKEY from \n" +
                    "(select c.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_excl_delta as c union all \n" +
                    "select d.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work as d) \n" +
                    " order by rand() limit %s";

    public static String GET_WORK_REC_DIFF_DELTACURR_EXCL =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",subtitle as SUBTITLE \n" +
                    ",u_key as UKEY \n" +
                    ",volumeno as VOLUMENO \n" +
                    ",copyrightyear as COPYRIGHTYEAR \n" +
                    ",editionno as EDITIONNO \n" +
                    ",pmc as PMC \n" +
                    ",work_type as WORKTYPE \n" +
                    ",statuscode as STATUSCODE \n" +
                    ",imprintcode as IMPRINTCODE \n" +
                    ",te_opco as TEOPCO \n" +
                    ",opco as OPCO \n" +
                    ",resp_centre as RESPCENTRE \n" +
                    ",pmg as PMG \n" +
                    ",languagecode as LANGUAGECODE \n" +
                    ",electro_rights_indicator as ELECTRORIGHTSINDICATOR \n" +
                    ",f_oa_journal_type as FOAJOURNALTYPE \n" +
                    ",f_society_ownership as FSOCIETYOWNERSHIP \n" +
                    ",subscription_type as SUBSCRIPTIONTYPE from \n" +
                    "(select c.u_key,c.sourceref,c.title,c.subtitle,c.volumeno,c.copyrightyear,c.editionno,c.pmc\n" +
                    ",c.work_type,c.statuscode,c.imprintcode,c.opco,c.resp_centre,c.te_opco,\n" +
                    "c.pmg,c.languagecode,c.electro_rights_indicator,\n" +
                    "c.f_oa_journal_type,c.f_society_ownership,c.subscription_type from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_excl_delta as c union all \n" +
                    "select d.u_key,d.sourceref,d.title,d.subtitle,d.volumeno,d.copyrightyear,d.editionno,d.pmc \n" +
                    ",d.work_type,d.statuscode,d.imprintcode,d.opco,d.resp_centre,d.te_opco,\n" +
                    "d.pmg,d.languagecode,d.electro_rights_indicator,d.f_oa_journal_type,d.f_society_ownership,d.subscription_type \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work as d) \n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_RANDOM_WORK_IDENT_KEY_DIFF_DELTACURR_EXCL =
            "select u_key as UKEY from \n" +
                    "(select c.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_excl_delta as c union all \n" +
                    "select d.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_identifier as d) \n" +
                    " order by rand() limit %s";

    public static String GET_WORK_IDENT_REC_DIFF_DELTACURR_EXCL =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE from \n" +
                    "(select c.u_key,c.sourceref,c.identifier,c.identifier_type from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_excl_delta as c union all \n" +
                    "select d.u_key,d.sourceref,d.identifier,d.identifier_type \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_identifier as d) \n" +
                    " where u_key in ('%s') order by u_key desc";


    public static String GET_ACCPROD_REC_DIFF_DELTACURR_CURRHIST =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR from \n" +
                    "(select c.u_key,c.sourceref,c.accountableproduct,c.accountablename,c.accountableparent,c.dq_err from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_accountable_product d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_part c ))\n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_ACCPROD_REC_EXCL_DELTA =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_accountable_product_excl_delta \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_MANIF_REC_DIFF_DELTACURR_CURRHIST =
            "select sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",title as TITLE \n" +
                    ",intereditionflag as INTEREDITIONFLAG \n" +
                    ",firstpublisheddate as FIRSTPUBLISHEDDATE \n" +
                    ",binding as BINDING \n" +
                    ",manifestation_type as MANIFESTATIONTYPE \n" +
                    ",status as STATUS \n" +
                    ",work_id as WORKID \n" +
                    ",last_pub_date as LASTPUBDATE \n" +
                    ",dq_err as DQ_ERR from \n" +
                    "(select c.u_key,c.sourceref,c.title,c.intereditionflag,c.firstpublisheddate,c.binding,c.manifestation_type,c.status,c.work_id,c.last_pub_date,c.dq_err from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_part c ))\n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_MANIF_REC_EXCL_DELTA =
            "select sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",title as TITLE \n" +
                    ",intereditionflag as INTEREDITIONFLAG \n" +
                    ",firstpublisheddate as FIRSTPUBLISHEDDATE \n" +
                    ",binding as BINDING \n" +
                    ",manifestation_type as MANIFESTATIONTYPE \n" +
                    ",status as STATUS \n" +
                    ",work_id as WORKID \n" +
                    ",last_pub_date as LASTPUBDATE \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_excl_delta \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_MANIF_KEY_DIFF_DELTACURR_CURRHIST =
            "select u_key as UKEY from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_part c ))\n" +
                    " order by rand() limit %s";

    public static String GET_RANDOM_MANIF_IDENT_KEY_DIFF_DELTACURR_CURRHIST =
            "select u_key as UKEY from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation_identifier d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part c ))\n" +
                    " order by rand() limit %s";

    public static String GET_MANIF_IDENT_REC_DIFF_DELTACURR_CURRHIST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE from \n" +
                    "(select c.u_key,c.sourceref,c.identifier,c.identifier_type from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_manifestation_identifier d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_part c ))\n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_MANIF_IDENT_REC_EXCL_DELTA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_manifestation_identifier_excl_delta \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_PRODUCT_KEY_DIFF_DELTACURR_CURRHIST =
            "select u_key as UKEY from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_product d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_part c ))\n" +
                    " order by rand() limit %s";

    public static String GET_PRODUCT_DIFF_DELTACURR_CURRHIST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",bindingcode as BINDINGCODE \n" +
                    ",name as NAME \n" +
                    ",shorttitle as SHORTTITLE \n" +
                    ",launchdate as LAUNCHDATE \n" +
                    ",taxcode as TAXCODE \n" +
                    ",status as STATUS \n" +
                    ",manifestationref as MANIFESTATIONREF \n" +
                    ",worksource as WORKSOURCE \n" +
                    ",work_type as WORKTYPE \n" +
                    ",separately_sale_indicator as SEPRATELYSALEINDICATOR \n" +
                    ",trial_allowed_indicator as TRIALALLOWEDINDICATOR \n" +
                    ",f_work_source_ref as FWORKSOURCEREF \n" +
                    ",product_type as PRODUCTTYPE \n" +
                    ",f_revenue_model as REVENUEMODEL \n" +
                    ",dq_err as DQ_ERR from \n" +
                    "(select c.sourceref,c.bindingcode,c.u_key,c.name,c.shorttitle,c.launchdate,c.taxcode,c.status,c.manifestationref,\n" +
                    "c.worksource,c.work_type,c.separately_sale_indicator,c.trial_allowed_indicator,c.f_work_source_ref,\n" +
                    "c.product_type,c.f_revenue_model,c.dq_err  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_product d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_part c ))\n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_PRODUCT_REC_EXCL_DELTA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",bindingcode as BINDINGCODE \n" +
                    ",name as NAME \n" +
                    ",shorttitle as SHORTTITLE \n" +
                    ",launchdate as LAUNCHDATE \n" +
                    ",taxcode as TAXCODE \n" +
                    ",status as STATUS \n" +
                    ",manifestationref as MANIFESTATIONREF \n" +
                    ",worksource as WORKSOURCE \n" +
                    ",work_type as WORKTYPE \n" +
                    ",separately_sale_indicator as SEPRATELYSALEINDICATOR \n" +
                    ",trial_allowed_indicator as TRIALALLOWEDINDICATOR \n" +
                    ",f_work_source_ref as FWORKSOURCEREF \n" +
                    ",product_type as PRODUCTTYPE \n" +
                    ",f_revenue_model as REVENUEMODEL \n" +
                    ",dq_err as DQ_ERR  \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_excl_delta \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";


    public static String GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_DELTACURR_CURRHIST =
            "select u_key as UKEY from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_person_role d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_part c ))\n" +
                    " order by rand() limit %s";

    public static String GET_WORK_PERS_ROLE_REC_DIFF_DELTACURR_CURRHIST =
            "select " +
                    "worksourceref as WORKSOURCEREF \n" +
                    ",personsourceref as PERSONSOURCEREF \n" +
                    ",roletype as ROLETYPE \n" +
                    ",u_key as UKEY \n" +
                    ",sequence as SEQUENCE \n" +
                    ",deduplicator as DEDUPLICATOR \n" +
                    ",dq_err as DQ_ERR from \n" +
                    "(select c.worksourceref,c.personsourceref,c.roletype,c.u_key,c.sequence,c.deduplicator,c.modifiedon,c.dq_err from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_person_role d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_part c ))\n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_PERS_ROLE_REC_EXCL_DELTA =
            "select " +
                    "worksourceref as WORKSOURCEREF \n" +
                    ",personsourceref as PERSONSOURCEREF \n" +
                    ",roletype as ROLETYPE \n" +
                    ",u_key as UKEY \n" +
                    ",sequence as SEQUENCE \n" +
                    ",deduplicator as DEDUPLICATOR \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_person_role_excl_delta \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_WORK_RELATION_KEY_DIFF_DELTACURR_CURRHIST =
            "select u_key as UKEY from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_relationship d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_part c ))\n" +
                    " order by rand() limit %s";

    public static String GET_WORK_RELATION_REC_DIFF_DELTACURR_CURRHIST =
            "select " +
                    "parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR from\n" +
                    "(select c.u_key,c.parentref,c.childref,c.relationtyperef,c.modifiedon,c.dq_err from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_relationship d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_part c ))\n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_RELATION_REC_EXCL_DELTA =
            "select " +
                    "parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_relationship_excl_delta \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";


    public static String GET_RANDOM_WORK_KEY_DIFF_DELTACURR_CURRHIST =
            "select u_key as UKEY from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_part c ))\n" +
                    " order by rand() limit %s";

    public static String GET_WORK_REC_DIFF_DELTACURR_CURRHIST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",subtitle as SUBTITLE \n" +
                    ",u_key as UKEY \n" +
                    ",volumeno as VOLUMENO \n" +
                    ",copyrightyear as COPYRIGHTYEAR \n" +
                    ",editionno as EDITIONNO \n" +
                    ",pmc as PMC \n" +
                    ",work_type as WORKTYPE \n" +
                    ",statuscode as STATUSCODE \n" +
                    ",imprintcode as IMPRINTCODE \n" +
                    ",te_opco as TEOPCO \n" +
                    ",opco as OPCO \n" +
                    ",resp_centre as RESPCENTRE \n" +
                    ",pmg as PMG \n" +
                    ",languagecode as LANGUAGECODE \n" +
                    ",electro_rights_indicator as ELECTRORIGHTSINDICATOR \n" +
                    ",f_oa_journal_type as FOAJOURNALTYPE \n" +
                    ",f_society_ownership as FSOCIETYOWNERSHIP \n" +
                    ",subscription_type as SUBSCRIPTIONTYPE from \n" +
                    "(select c.u_key,c.sourceref,c.title,c.subtitle,c.volumeno,c.copyrightyear,c.editionno,c.pmc\n" +
                    ",c.work_type,c.statuscode,c.imprintcode,c.opco,c.resp_centre,c.te_opco,\n" +
                    "c.pmg,c.languagecode,c.electro_rights_indicator,\n" +
                    "c.f_oa_journal_type,c.f_society_ownership,c.subscription_type from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_part c ))\n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_REC_DIFF_EXCL_DELTA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",title as TITLE \n" +
                    ",subtitle as SUBTITLE \n" +
                    ",u_key as UKEY \n" +
                    ",volumeno as VOLUMENO \n" +
                    ",copyrightyear as COPYRIGHTYEAR \n" +
                    ",editionno as EDITIONNO \n" +
                    ",pmc as PMC \n" +
                    ",work_type as WORKTYPE \n" +
                    ",statuscode as STATUSCODE \n" +
                    ",imprintcode as IMPRINTCODE \n" +
                    ",te_opco as TEOPCO \n" +
                    ",opco as OPCO \n" +
                    ",resp_centre as RESPCENTRE \n" +
                    ",pmg as PMG \n" +
                    ",languagecode as LANGUAGECODE \n" +
                    ",electro_rights_indicator as ELECTRORIGHTSINDICATOR \n" +
                    ",f_oa_journal_type as FOAJOURNALTYPE \n" +
                    ",f_society_ownership as FSOCIETYOWNERSHIP \n" +
                    ",subscription_type as SUBSCRIPTIONTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_excl_delta \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";


    public static String GET_RANDOM_WORK_IDENT_KEY_DIFF_DELTACURR_CURRHIST =
            "select u_key as UKEY from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_identifier d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_part c ))\n" +
                    " order by rand() limit %s";

    public static String GET_WORK_IDENT_REC_DIFF_DELTACURR_CURRHIST =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE from \n" +
                    "(select c.u_key,c.sourceref,c.identifier,c.identifier_type from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_work_identifier d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_part c ))\n" +
                    " where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_IDENT_REC_EXCL_DELTA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_work_identifier_excl_delta \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_PERSON_KEY_DIFF_DELTACURR_CURRHIST =
            "select u_key as UKEY from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_person d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_part c ))\n" +
                    " order by rand() limit %s";

    public static String GET_PERSON_REC_DIFF_DELTACURR_CURRHIST =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",u_key as UKEY" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQ_ERR from " +
                    "(select c.u_key,c.sourceref,c.firstname,c.familyname,c.peoplehub_id,c.email_address,c.dq_err from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_person d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_part c ))\n" +
                    " where u_key in (%s) order by u_key desc";

    public static String GET_PERSON_REC_EXCL_DELTA =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",u_key as UKEY" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQ_ERR " +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_excl_delta \n" +
                    "where u_key in (%s) \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_PERSON_KEY_DIFF_DELTACURR_EXCL =
            "select u_key as UKEY from \n" +
                    "(select c.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_excl_delta as c union all \n" +
                    "select d.u_key \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_person as d) \n" +
                    " order by rand() limit %s";



    public static String GET_PERSON_REC_DIFF_DELTACURR_EXCL =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",u_key as UKEY" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQ_ERR from " +
                    "(select c.u_key,c.sourceref,c.firstname,c.familyname,c.peoplehub_id,c.email_address,c.dq_err from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_excl_delta as c union all \n" +
                    "select d.u_key,d.sourceref,d.firstname,d.familyname,d.peoplehub_id,d.email_address,d.dq_err \n" +
                    " from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_person as d) \n" +
                    " where u_key in (%s) order by u_key desc";

    public static String GET_PERSON_REC_LATEST =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",u_key as UKEY" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQ_ERR " +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_latest \n" +
                    "where u_key in (%s) \n" +
                    "order by u_key desc";















}






