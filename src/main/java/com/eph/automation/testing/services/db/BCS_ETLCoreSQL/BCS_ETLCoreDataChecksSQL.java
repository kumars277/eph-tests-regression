package com.eph.automation.testing.services.db.BCS_ETLCoreSQL;




public class BCS_ETLCoreDataChecksSQL {

    public static String GET_RANDOM_ACCPROD_KEY_INBOUND =
    "SELECT u_key as sourceref\n" +
            "FROM\n" +
            "  (\n" +
            "   SELECT DISTINCT\n" +
            "     NULLIF(sourceref, '') sourceref\n" +
            "   , NULLIF(CAST(accountableproduct AS varchar), '') accountableproduct\n" +
            "   , NULLIF(accountablename, '') accountablename\n" +
            "   , NULLIF(accountableparent, '') accountableparent\n" +
            "   , concat(NULLIF(sourceref, ''), NULLIF(accountableparent, '')) u_key\n" +
            "   , 'N' dq_err\n" +
            "   FROM\n" +
            "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification\n" +
            "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".worktypecode ON (COALESCE(split_part(classification.value, ' | ', 1), 'DEFAULT') = ppmcode))\n" +
            "   WHERE (classification.classificationcode = 'DCDFAC | Accounting class')\n" +
            ")  A\n" +
            "WHERE ((A.sourceref IS NOT NULL) AND (A.accountableparent IS NOT NULL)) order by rand() limit %s";

    public static String GET_ACCPROD_REC_INBOUND_DATA =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "FROM \n" +
                    "(\n" +
                    "      SELECT DISTINCT \n" +
                    "        NULLIF(sourceref, '') sourceref \n" +
                    "      , NULLIF(CAST(accountableproduct AS varchar), '') accountableproduct \n" +
                    "      , NULLIF(accountablename, '') accountablename \n" +
                    "      , NULLIF(accountableparent, '') accountableparent \n" +
                    "      , concat(NULLIF(sourceref, ''), NULLIF(accountableparent, '')) u_key \n" +
                    "   , 'N' dq_err\n" +
                    "   FROM\n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".worktypecode ON (COALESCE(split_part(classification.value, ' | ', 1), 'DEFAULT') = ppmcode))\n" +
                    "   WHERE (classification.classificationcode = 'DCDFAC | Accounting class')\n" +
                    ")  A \n" +
                    "WHERE ((A.sourceref IS NOT NULL) AND (A.accountableparent IS NOT NULL)) and sourceref in ('%s') order by sourceref desc";

    public static String GET_RANDOM_WRK_PERS_KEY_INBOUND =
            "SELECT u_key as sourceref\n" +
                    " FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(sourceref,'') worksourceref\n" +
                    "   , NULLIF(CAST(businesspartnerid AS varchar),'') personsourceref\n" +
                    "   , NULLIF(CAST(old_businesspartnerid AS varchar),'') linking_id\n" +
                    "   , NULLIF(rolecode.ephcode,'') roletype\n" +
                    "   , NULLIF(sourceref,'')||NULLIF(rolecode.ephcode,'')||NULLIF(CAST(businesspartnerid AS varchar),'') as u_key\n" +
                    "   , NULLIF(CAST(sequence AS varchar),'') sequence\n" +
                    "   , NULLIF(CAST(locationid AS varchar),'') deduplicator\n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    "   , 'N' dq_err\n" +
                    "FROM\n" +
                    "((SELECT sourceref,\n" +
                    " lower(to_hex(md5(to_utf8(concat(cast(businesspartnerid as varchar)" +
                    ",TRIM(UPPER((CASE WHEN (isperson = 'N') THEN department ELSE firstname END)))" +
                    ",TRIM(UPPER((CASE WHEN (isperson = 'N') THEN institution ELSE lastname END)))))))) businesspartnerid, \n" +
                    "businesspartnerid old_businesspartnerid,\n" +
                    "copyrightholdertype,\n" +
                    "sequence,\n" +
                    "metamodifiedon,\n" +
                    "locationid,\n" +
                    "row_number()\n" +
                    "OVER (partition by sourceref,businesspartnerid,copyrightholdertype\n" +
                    "ORDER BY metamodifiedon,sequence) min_id\n" +
                    "FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originators)\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(copyrightholdertype, ' | ', 1) = rolecode.ppmcode))\n" +
                    "where min_id = 1\n" +
                    "UNION \n" +
                    "SELECT\n" +
                    "     NULLIF(r.sourceref,'') worksourceref\n" +
                    "   , NULLIF(lower(to_hex(md5(to_utf8(w.peoplehub_id)))),'') personsourceref \n" +
                    "   , NULLIF(w.peoplehub_id,'') linking_id \n" +
                    "   , NULLIF(rolecode.ephcode,'') roletype\n" +
                    "   , r.sourceref||rolecode.ephcode||lower(to_hex(md5(to_utf8(w.peoplehub_id)))) as u_key \n" +
                    "   , (CASE WHEN (substr(split_part(NULLIF(responsibility,''), ' | ', 1), -1, 1) = '2') THEN '2' ELSE '1' END) sequence\n" +
                    "   , '0' deduplicator\n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    "   , 'N' dq_err\n" +
                    "   FROM\n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_responsibilities r\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".workday_reference_v w on r.email = w.email \n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(responsibility, ' | ', 1) = rolecode.ppmcode)) \n" +
                    ") A\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily vf on a.worksourceref = vf.workmasterprojectno and a.worksourceref = vf.sourceref\n" +
                    "WHERE (((A.worksourceref IS NOT NULL) AND (A.personsourceref IS NOT NULL)) AND (A.roletype IS NOT NULL)) \n"+
                   "order by rand() limit %s \n";

    public static String GET_RANDOM_WORK_KEY_INBOUND =
            "SELECT u_key as sourceref \n" +
                    "FROM\n" +
                    "(\n" +
                    "SELECT DISTINCT\n" +
                    "content.sourceref sourceref\n" +
                    ", content.sourceref u_key\n" +
                    ", content.title title\n" +
                    ", content.subtitle subtitle\n" +
                    ", content.volumeno volumeno\n" +
                    ", content.copyrightyear copyrightyear\n" +
                    ", content.editionno editionno\n" +
                    ", content.pmc pmc\n" +
                    ", COALESCE(NULLIF(worktypecode.ephcode, ''), 'UNK') work_type\n" +
                    ", CASE WHEN content.metadeleted = 'Y' THEN 'NVW' ELSE COALESCE(NULLIF(work_status.eph_work_status_code, ''), 'UNK') END statuscode\n" +
                    ", content.imprintcode imprintcode\n" +
                    ", NULLIF(cast(opcocode.\"11icode\" as varchar), '') te_opco\n" +
                    ", NULLIF(cast(opcocode.r12code as varchar), '') opco\n" +
                    ", NULLIF(cast(coalesce(rc_pmg_co.rc,rc_pmg.rc) as varchar), '') resp_centre\n" +
                    ", content.pmg pmg\n" +
                    ", NULLIF(languagecode.ephcode, '') languagecode\n" +
                    ", CASE WHEN erights.value = 'Y | Yes' THEN true WHEN erights.value = 'N | No' THEN false ELSE null END electro_rights_indicator\n" +
                    ", 'N' f_oa_journal_type \n" +
                    ", CAST(NULL AS varchar) f_society_ownership\n" +
                    ", CAST(NULL AS varchar) subscription_type\n" +
                    ", date_parse(NULLIF(content.metamodifiedon, ''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "NULLIF(sourceref, '') sourceref\n" +
                    ", NULLIF(sourceref, '') u_key\n" +
                    ", NULLIF(title, '') title\n" +
                    ", NULLIF(subtitle, '') subtitle\n" +
                    ", NULLIF(volumeno, '') volumeno\n" +
                    ", metadeleted metadeleted\n" +
                    ", copyrightyear copyrightyear\n" +
                    ", editionno editionno\n" +
                    ", substring(split_part(NULLIF(subgroup, ''), ' | ', 1), 2, 3) pmc\n" +
                    ", split_part(NULLIF(imprint, ''), ' | ', 1) imprintcode\n" +
                    ", substring(split_part(NULLIF(division, ''), ' | ', 1), 2, 3) pmg\n" +
                    ", date_parse(NULLIF(metamodifiedon, ''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    ", split_part(language, ' | ', 1) language\n" +
                    ", ownership ownership\n" +
                    ", metamodifiedon metamodifiedon\n" +
                    "FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content) content\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product on content.sourceref = product.sourceref\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily versionfamily ON (content.sourceref = versionfamily.workmasterprojectno)\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification ON ((content.sourceref = classification.sourceref) AND (classification.classificationcode = 'DCDFAC | Accounting class'))\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification erights ON content.sourceref = erights.sourceref AND erights.classificationcode = 'PAERIGHTS | Electronic rights' \n"+
                    "LEFT JOIN (SELECT workmasterprojectno, min(manifestation_priority) work_priority FROM (  \n" +
                    "    SELECT m.sourceref, w.workmasterprojectno, coalesce(nullif(m.ref_key_manifestation_priority,6),nullif(m.delivery_status_manifestation_priority,6),nullif(m.delta_status_manifestation_priority,6),6) manifestation_priority\n" +
                    "    FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".all_manifestation_statuses_v m\n" +
                    "    JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily w on m.sourceref = w.sourceref) group by workmasterprojectno) work_priority on work_priority.workmasterprojectno = content.sourceref\n" +
                    "LEFT JOIN (SELECT DISTINCT eph_work_status_code, manifestation_priority FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode) work_status on work_priority.work_priority = work_status.manifestation_priority\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".opcocode opcocode ON (content.ownership = opcocode.ppmcode)\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".pmgtorcmapping rc_pmg_co ON opcocode.\"11icode\" = rc_pmg_co.company and cast(content.pmg as integer) = cast(rc_pmg_co.pmg as integer) and rc_pmg_co.active_end_date is null\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".pmgtorcmapping rc_pmg ON NULLIF(rc_pmg.company,'') is null and cast(content.pmg as integer) = cast(rc_pmg.pmg as integer) and rc_pmg.active_end_date is null\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".languagecode languagecode ON (content.language = languagecode.ppmcode)\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".worktypecode worktypecode ON (\n" +
                    "    coalesce(CASE WHEN split_part(product.versiontype, ' | ',1) in ('COMB','MVOL','NBS')\n" +
                    "                  THEN split_part(product.versiontype, ' | ',1) else split_part(classification.value, ' | ', 1) END,'DEFAULT') = worktypecode.ppmcode)\n" +
                    "UNION ALL\n" +
                    "SELECT DISTINCT\n" +
                    "content.seriesid sourceref\n" +
                    ", content.seriesid u_key\n" +
                    ", content.title title\n" +
                    ", cast(null as varchar) subtitle\n" +
                    ", cast(null as varchar) volumeno\n" +
                    ", cast(null as integer) copyrightyear\n" +
                    ", cast(null as integer) editionno\n" +
                    ", cast(null as varchar) pmc\n" +
                    ", 'BKS' work_type\n" +
                    ", COALESCE(NULLIF(work_status.eph_work_status_code, ''), 'UNK') statuscode\n" +
                    ", cast(null as varchar) imprintcode\n" +
                    ", cast(null as varchar) te_opco\n" +
                    ", cast(null as varchar) opco\n" +
                    ", cast(null as varchar) resp_centre\n" +
                    ", cast(null as varchar) pmg\n" +
                    ", cast(null as varchar) languagecode\n" +
                    ", CAST(NULL AS boolean) electro_rights_indicator" +
                    ", 'N' f_oa_journal_type \n" +
                    ", CAST(NULL AS varchar) f_society_ownership\n" +
                    ", CAST(NULL AS varchar) subscription_type\n" +
                    ", modifiedon modifiedon\n" +
                    "FROM\n" +
                    "(SELECT\n" +
                    "seriesid seriesid\n" +
                    ", series title\n" +
                    ", MAX(date_parse(NULLIF(metamodifiedon, ''),'%%d-%%b-%%Y %%H:%%i:%%s')) modifiedon\n" +
                    "FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content group by seriesid, series) content\n" +
                    "LEFT JOIN (select seriesid, min(manifestation_priority) work_priority FROM\n" +
                    "    (select m.sourceref, w.seriesid, coalesce(nullif(m.ref_key_manifestation_priority,6),nullif(m.delivery_status_manifestation_priority,6),nullif(m.delta_status_manifestation_priority,6),6) manifestation_priority\n" +
                    "    FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".all_manifestation_statuses_v m\n" +
                    "    JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w on m.sourceref = w.sourceref where w.seriesid != '') group by seriesid) work_priority on content.seriesid = work_priority.seriesid\n" +
                    "LEFT JOIN (SELECT DISTINCT eph_work_status_code, manifestation_priority FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode) work_status on work_priority.work_priority = work_status.manifestation_priority\n" +
                    "    ) A\n" +
                    "WHERE (A.sourceref IS NOT NULL)order by rand() limit %s";

    public static String GET_WORK_INBOUND_DATA =
            "select " +
                    "u_key as UKEY \n" +
                    ",sourceref as SOURCEREF \n" +
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
                    "FROM\n" +
                    "(\n" +
                    "SELECT DISTINCT\n" +
                    "content.sourceref sourceref\n" +
                    ", content.sourceref u_key\n" +
                    ", content.title title\n" +
                    ", content.subtitle subtitle\n" +
                    ", content.volumeno volumeno\n" +
                    ", content.copyrightyear copyrightyear\n" +
                    ", content.editionno editionno\n" +
                    ", content.pmc pmc\n" +
                    ", COALESCE(NULLIF(worktypecode.ephcode, ''), 'UNK') work_type\n" +
                    ", CASE WHEN content.metadeleted = 'Y' THEN 'NVW' ELSE COALESCE(NULLIF(work_status.eph_work_status_code, ''), 'UNK') END statuscode\n" +
                    ", content.imprintcode imprintcode\n" +
                    ", NULLIF(cast(opcocode.\"11icode\" as varchar), '') te_opco\n" +
                    ", NULLIF(cast(opcocode.r12code as varchar), '') opco\n" +
                    ", NULLIF(cast(coalesce(rc_pmg_co.rc,rc_pmg.rc) as varchar), '') resp_centre\n" +
                    ", content.pmg pmg\n" +
                    ", NULLIF(languagecode.ephcode, '') languagecode\n" +
                    ", CASE WHEN erights.value = 'Y | Yes' THEN true WHEN erights.value = 'N | No' THEN false ELSE null END electro_rights_indicator\n" +
                    ", 'N' f_oa_journal_type \n" +
                    ", CAST(NULL AS varchar) f_society_ownership\n" +
                    ", CAST(NULL AS varchar) subscription_type\n" +
                    ", date_parse(NULLIF(content.metamodifiedon, ''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    "FROM (\n" +
                    "SELECT\n" +
                    "NULLIF(sourceref, '') sourceref\n" +
                    ", NULLIF(sourceref, '') u_key\n" +
                    ", NULLIF(title, '') title\n" +
                    ", NULLIF(subtitle, '') subtitle\n" +
                    ", NULLIF(volumeno, '') volumeno\n" +
                    ", metadeleted metadeleted\n" +
                    ", copyrightyear copyrightyear\n" +
                    ", editionno editionno\n" +
                    ", substring(split_part(NULLIF(subgroup, ''), ' | ', 1), 2, 3) pmc\n" +
                    ", split_part(NULLIF(imprint, ''), ' | ', 1) imprintcode\n" +
                    ", substring(split_part(NULLIF(division, ''), ' | ', 1), 2, 3) pmg\n" +
                    ", date_parse(NULLIF(metamodifiedon, ''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    ", split_part(language, ' | ', 1) language\n" +
                    ", ownership ownership\n" +
                    ", metamodifiedon metamodifiedon\n" +
                    "FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content) content\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product on content.sourceref = product.sourceref\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily versionfamily ON (content.sourceref = versionfamily.workmasterprojectno)\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification ON ((content.sourceref = classification.sourceref) AND (classification.classificationcode = 'DCDFAC | Accounting class'))\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification erights ON content.sourceref = erights.sourceref AND erights.classificationcode = 'PAERIGHTS | Electronic rights' \n"+
                    "LEFT JOIN (SELECT workmasterprojectno, min(manifestation_priority) work_priority FROM (  \n" +
                    "    SELECT m.sourceref, w.workmasterprojectno, coalesce(nullif(m.ref_key_manifestation_priority,6),nullif(m.delivery_status_manifestation_priority,6),nullif(m.delta_status_manifestation_priority,6),6) manifestation_priority\n" +
                    "    FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".all_manifestation_statuses_v m\n" +
                    "    JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily w on m.sourceref = w.sourceref) group by workmasterprojectno) work_priority on work_priority.workmasterprojectno = content.sourceref\n" +
                    "LEFT JOIN (SELECT DISTINCT eph_work_status_code, manifestation_priority FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode) work_status on work_priority.work_priority = work_status.manifestation_priority\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".opcocode opcocode ON (content.ownership = opcocode.ppmcode)\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".pmgtorcmapping rc_pmg_co ON opcocode.\"11icode\" = rc_pmg_co.company and cast(content.pmg as integer) = cast(rc_pmg_co.pmg as integer) and rc_pmg_co.active_end_date is null\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".pmgtorcmapping rc_pmg ON NULLIF(rc_pmg.company,'') is null and cast(content.pmg as integer) = cast(rc_pmg.pmg as integer) and rc_pmg.active_end_date is null\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".languagecode languagecode ON (content.language = languagecode.ppmcode)\n" +
                    "LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".worktypecode worktypecode ON (\n" +
                    "    coalesce(CASE WHEN split_part(product.versiontype, ' | ',1) in ('COMB','MVOL','NBS')\n" +
                    "                  THEN split_part(product.versiontype, ' | ',1) else split_part(classification.value, ' | ', 1) END,'DEFAULT') = worktypecode.ppmcode)\n" +
                    "UNION ALL\n" +
                    "SELECT DISTINCT\n" +
                    "content.seriesid sourceref\n" +
                    ", content.seriesid u_key\n" +
                    ", content.title title\n" +
                    ", cast(null as varchar) subtitle\n" +
                    ", cast(null as varchar) volumeno\n" +
                    ", cast(null as integer) copyrightyear\n" +
                    ", cast(null as integer) editionno\n" +
                    ", cast(null as varchar) pmc\n" +
                    ", 'BKS' work_type\n" +
                    ", COALESCE(NULLIF(work_status.eph_work_status_code, ''), 'UNK') statuscode\n" +
                    ", cast(null as varchar) imprintcode\n" +
                    ", cast(null as varchar) te_opco\n" +
                    ", cast(null as varchar) opco\n" +
                    ", cast(null as varchar) resp_centre\n" +
                    ", cast(null as varchar) pmg\n" +
                    ", cast(null as varchar) languagecode\n" +
                    ", CAST(NULL AS boolean) electro_rights_indicator" +
                    ", 'N' f_oa_journal_type \n" +
                    ", CAST(NULL AS varchar) f_society_ownership\n" +
                    ", CAST(NULL AS varchar) subscription_type\n" +
                    ", modifiedon modifiedon\n" +
                    "FROM\n" +
                    "(SELECT\n" +
                    "seriesid seriesid\n" +
                    ", series title\n" +
                    ", MAX(date_parse(NULLIF(metamodifiedon, ''),'%%d-%%b-%%Y %%H:%%i:%%s')) modifiedon\n" +
                    "FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content group by seriesid, series) content\n" +
                    "LEFT JOIN (select seriesid, min(manifestation_priority) work_priority FROM\n" +
                    "    (select m.sourceref, w.seriesid, coalesce(nullif(m.ref_key_manifestation_priority,6),nullif(m.delivery_status_manifestation_priority,6),nullif(m.delta_status_manifestation_priority,6),6) manifestation_priority\n" +
                    "    FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".all_manifestation_statuses_v m\n" +
                    "    JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w on m.sourceref = w.sourceref where w.seriesid != '') group by seriesid) work_priority on content.seriesid = work_priority.seriesid\n" +
                    "LEFT JOIN (SELECT DISTINCT eph_work_status_code, manifestation_priority FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode) work_status on work_priority.work_priority = work_status.manifestation_priority\n" +
                    "    ) A\n" +
                    "WHERE (A.sourceref IS NOT NULL) and u_key in ('%s') order by u_key desc";

    public static String GET_WORK_PERS_INBOUND_DATA =
            "select " +
                    "worksourceref as WORKSOURCEREF \n" +
                    ",personsourceref as PERSONSOURCEREF \n" +
                    ",roletype as ROLETYPE \n" +
                    ",u_key as UKEY \n" +
                    ",sequence as SEQUENCE \n" +
                    ",deduplicator as DEDUPLICATOR \n" +
                    ",dq_err as DQ_ERR \n" +
                    " FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(sourceref,'') worksourceref\n" +
                    "   , NULLIF(CAST(businesspartnerid AS varchar),'') personsourceref\n" +
                    "   , NULLIF(CAST(old_businesspartnerid AS varchar),'') linking_id\n" +
                    "   , NULLIF(rolecode.ephcode,'') roletype\n" +
                    "   , NULLIF(sourceref,'')||NULLIF(rolecode.ephcode,'')||NULLIF(CAST(businesspartnerid AS varchar),'') as u_key\n" +
                    "   , NULLIF(CAST(sequence AS varchar),'') sequence\n" +
                    "   , NULLIF(CAST(locationid AS varchar),'') deduplicator\n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    "   , 'N' dq_err\n" +
                    "FROM\n" +
                    "((SELECT sourceref,\n" +
                    " lower(to_hex(md5(to_utf8(concat(cast(businesspartnerid as varchar)" +
                    ",TRIM(UPPER((CASE WHEN (isperson = 'N') THEN department ELSE firstname END)))" +
                    ",TRIM(UPPER((CASE WHEN (isperson = 'N') THEN institution ELSE lastname END)))))))) businesspartnerid, \n" +
                    "businesspartnerid old_businesspartnerid,\n" +
                    "copyrightholdertype,\n" +
                    "sequence,\n" +
                    "metamodifiedon,\n" +
                    "locationid,\n" +
                    "row_number()\n" +
                    "OVER (partition by sourceref,businesspartnerid,copyrightholdertype\n" +
                    "ORDER BY metamodifiedon,sequence) min_id\n" +
                    "FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originators)\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(copyrightholdertype, ' | ', 1) = rolecode.ppmcode))\n" +
                    "where min_id = 1\n" +
                    "UNION \n" +
                    "SELECT\n" +
                    "     NULLIF(r.sourceref,'') worksourceref\n" +
                    "   , NULLIF(lower(to_hex(md5(to_utf8(w.peoplehub_id)))),'') personsourceref \n" +
                    "   , NULLIF(w.peoplehub_id,'') linking_id \n" +
                    "   , NULLIF(rolecode.ephcode,'') roletype\n" +
                    "   , r.sourceref||rolecode.ephcode||lower(to_hex(md5(to_utf8(w.peoplehub_id)))) as u_key \n" +
                    "   , (CASE WHEN (substr(split_part(NULLIF(responsibility,''), ' | ', 1), -1, 1) = '2') THEN '2' ELSE '1' END) sequence\n" +
                    "   , '0' deduplicator\n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    "   , 'N' dq_err\n" +
                    "   FROM\n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_responsibilities r\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getDL_CoreViewDataBase()+".workday_reference_v w on r.email = w.email \n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(responsibility, ' | ', 1) = rolecode.ppmcode)) \n" +
                    ") A\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily vf on a.worksourceref = vf.workmasterprojectno and a.worksourceref = vf.sourceref\n" +
                    "WHERE (((A.worksourceref IS NOT NULL) AND (A.personsourceref IS NOT NULL)) AND (A.roletype IS NOT NULL)) \n"+
                    "and u_key in ('%s') order by u_key desc";


    public static String GET_RANDOM_MANIF_KEY_INBOUND =
                    " SELECT u_key as sourceref FROM (\n" +
                    "   SELECT DISTINCT\n" +
                    "     NULLIF(product.sourceref,'') sourceref\n" +
                    "   , NULLIF(product.sourceref,'') u_key\n" +
                    "   , NULLIF(content.title,'') title\n" +
                    "   , (CASE WHEN (NULLIF(intedition.classificationcode,'') IS NULL) THEN false ELSE true END) intereditionflag\n" +
                    "   , COALESCE(pubdates.min_actual_pubdate, pubdates.min_planned_pubdate) firstpublisheddate\n" +
                    "   , NULLIF(product.binding,'') binding\n" +
                    "   , NULLIF(manifestationtypecode.ephcode,'') manifestation_type\n" +
                    "   , CASE WHEN product.metadeleted = 'Y' THEN 'NVM' ELSE COALESCE(NULLIF(manifestationstatus.eph_manifestation_status_code,''),'UNK') END status \n" +
                    "   , NULLIF(workprod.workmasterprojectno,'') work_id \n" +
                    "   , CAST(NULL AS timestamp) last_pub_date \n" +
                    "   , 'N' dq_err\n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily workprod ON ((product.sourceref = workprod.sourceref) AND (workprod.workmasterprojectno IS NOT NULL))\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content ON (workprod.workmasterprojectno = content.sourceref) \n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        sourceref\n" +
                    "      , classificationcode\n" +
                    "      FROM\n" +
                    "        "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification\n" +
                    "      WHERE (classificationcode LIKE 'PARELIE%%')\n" +
                    "   )  intedition ON (product.sourceref = intedition.sourceref)\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".all_manifestation_pubdates_v pubdates ON product.sourceref = pubdates.sourceref\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".all_manifestation_statuses_v all_status ON (product.sourceref = all_status.sourceref)\n" +
                    "   LEFT JOIN (SELECT DISTINCT eph_manifestation_status_code, manifestation_priority FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode) manifestationstatus\n" +
                    "      ON coalesce(nullif(all_status.ref_key_manifestation_priority,6),nullif(all_status.delivery_status_manifestation_priority,6),nullif(all_status.delta_status_manifestation_priority,6),6) = manifestationstatus.manifestation_priority\n" +
                    "   LEFT JOIN (\n" +
                    "            select\n" +
                    "                 sourceref\n" +
                    "                ,case\n" +
                    "                    when producttype in ('199 | E-book-199', 'VAC | VAC - Voucher Access Card', 'INK | Inkling E-Book') then split_part(producttype,' | ',1)\n" +
                    "                    when binding in ('BB | Book/Hardback', 'BC | Book/Paperback') then split_part(binding,' | ',1)\n" +
                    "                    when versiontype in ('BKH | Book - Hardback', 'BKP | Book - Paperback') then split_part(versiontype,' | ',1)\n" +
                    "                    when deltabinding in ('1 | 1 - Hardback', '2 | 2 - Paperback') then split_part(deltabinding,' | ',1)\n" +
                    "                    when versiontype in ('NBP | Non - Book Physical', 'ELP | Electronic - Physical', 'ELO | Electronic - Online', 'COMB | Combination Set', 'MVOL | Multivol. Run-on Set') then split_part(versiontype,' | ',1)\n" +
                    "                    else '' end as manifestation_type\n" +
                    "            from\n" +
                    "                (select p.sourceref, p.versiontype, p.binding, a.value producttype, b.value deltabinding\n" +
                    "                 from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product p\n" +
                    "                 left outer join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification a on p.sourceref = a.sourceref and a.classificationcode like 'PTCO%%'\n" +
                    "                 left outer join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification b on p.sourceref = b.sourceref and b.classificationcode like 'PTDE%%')\n" +
                    "            ) manif_type on product.sourceref = manif_type.sourceref\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".manifestationtypecode ON manifestation_type = manifestationtypecode.ppmcode\n" +
                    ")A WHERE A.sourceref is not null order by rand() limit %s";


    public static String GET_MANIF_INBOUND_DATA =
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
                    " from (\n" +
                    "   SELECT DISTINCT\n" +
                    "     NULLIF(product.sourceref,'') sourceref\n" +
                    "   , NULLIF(product.sourceref,'') u_key\n" +
                    "   , NULLIF(content.title,'') title\n" +
                    "   , (CASE WHEN (NULLIF(intedition.classificationcode,'') IS NULL) THEN false ELSE true END) intereditionflag\n" +
                    "   , COALESCE(pubdates.min_actual_pubdate, pubdates.min_planned_pubdate) firstpublisheddate\n" +
                    "   , NULLIF(product.binding,'') binding\n" +
                    "   , NULLIF(manifestationtypecode.ephcode,'') manifestation_type\n" +
                    "   , CASE WHEN product.metadeleted = 'Y' THEN 'NVM' ELSE COALESCE(NULLIF(manifestationstatus.eph_manifestation_status_code,''),'UNK') END status\n" +
                    "   , NULLIF(workprod.workmasterprojectno,'') work_id\n" +
                    "   , CAST(NULL AS timestamp) last_pub_date\n" +
                    "   , 'N' dq_err\n" +
                    "   FROM\n" +
                    "     "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily workprod ON ((product.sourceref = workprod.sourceref) AND (workprod.workmasterprojectno IS NOT NULL))\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content ON (workprod.workmasterprojectno = content.sourceref) \n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT\n" +
                    "        sourceref\n" +
                    "      , classificationcode\n" +
                    "      FROM\n" +
                    "        "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification\n" +
                    "      WHERE (classificationcode LIKE 'PARELIE%%')\n" +
                    "   )  intedition ON (product.sourceref = intedition.sourceref)\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".all_manifestation_pubdates_v pubdates ON product.sourceref = pubdates.sourceref\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".all_manifestation_statuses_v all_status ON (product.sourceref = all_status.sourceref)\n" +
                    "   LEFT JOIN (SELECT DISTINCT eph_manifestation_status_code, manifestation_priority FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode) manifestationstatus\n" +
                    "      ON coalesce(nullif(all_status.ref_key_manifestation_priority,6),nullif(all_status.delivery_status_manifestation_priority,6),nullif(all_status.delta_status_manifestation_priority,6),6) = manifestationstatus.manifestation_priority\n" +
                    "   LEFT JOIN (\n" +
                    "            select\n" +
                    "                 sourceref\n" +
                    "                ,case\n" +
                    "                    when producttype in ('199 | E-book-199', 'VAC | VAC - Voucher Access Card', 'INK | Inkling E-Book') then split_part(producttype,' | ',1)\n" +
                    "                    when binding in ('BB | Book/Hardback', 'BC | Book/Paperback') then split_part(binding,' | ',1)\n" +
                    "                    when versiontype in ('BKH | Book - Hardback', 'BKP | Book - Paperback') then split_part(versiontype,' | ',1)\n" +
                    "                    when deltabinding in ('1 | 1 - Hardback', '2 | 2 - Paperback') then split_part(deltabinding,' | ',1)\n" +
                    "                    when versiontype in ('NBP | Non - Book Physical', 'ELP | Electronic - Physical', 'ELO | Electronic - Online', 'COMB | Combination Set', 'MVOL | Multivol. Run-on Set') then split_part(versiontype,' | ',1)\n" +
                    "                    else '' end as manifestation_type\n" +
                    "            from\n" +
                    "                (select p.sourceref, p.versiontype, p.binding, a.value producttype, b.value deltabinding\n" +
                    "                 from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product p\n" +
                    "                 left outer join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification a on p.sourceref = a.sourceref and a.classificationcode like 'PTCO%%'\n" +
                    "                 left outer join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification b on p.sourceref = b.sourceref and b.classificationcode like 'PTDE%%')\n" +
                    "            ) manif_type on product.sourceref = manif_type.sourceref\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".manifestationtypecode ON manifestation_type = manifestationtypecode.ppmcode\n" +
                    ")A WHERE A.sourceref is not null and u_key in ('%s') order by u_key desc \n";


    public static String GET_RANDOM_PERSON_KEY_INBOUND =
            "SELECT u_key as sourceref \n" +
                    "FROM \n" +
                    "  ( \n" +
                    "   SELECT DISTINCT \n" +
                    "     businesspartnerid sourceref \n" +
                    "   , lower (to_hex(md5(to_utf8(concat(cast(businesspartnerid as varchar)" +
                    "   ,TRIM(UPPER((CASE WHEN (isperson = 'N') THEN department ELSE firstname END)))" +
                    "   ,TRIM(UPPER((CASE WHEN (isperson = 'N') THEN institution ELSE lastname END))))))))u_key \n" +
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
                ",u_key as UKEY" +
                ",firstname as FIRSTNAME" +
                ",familyname as FAMILYNAME" +
                ",peoplehub_id as PEOPLEHUBID" +
                ",email_address as EMAIL" +
                ",dq_err as DQ_ERR " +
                "FROM( \n" +
                "   SELECT DISTINCT\n" +
                "     businesspartnerid sourceref \n" +
                "   , lower (to_hex(md5(to_utf8(concat(cast(businesspartnerid as varchar)" +
                "   ,TRIM(UPPER((CASE WHEN (isperson = 'N') THEN department ELSE firstname END)))" +
                "   ,TRIM(UPPER((CASE WHEN (isperson = 'N') THEN institution ELSE lastname END))))))))u_key \n" +
                "   , (CASE WHEN (isperson = 'N') THEN NULLIF(department, '') ELSE NULLIF(firstname, '') END) firstname \n" +
                "   , (CASE WHEN (isperson = 'N') THEN NULLIF(institution, '') ELSE NULLIF(lastname, '') END) familyname \n" +
                "   , CAST(null AS varchar) peoplehub_id \n" +
                "   , CAST(null AS varchar) email_address \n" +
                "   , 'N' dq_err \n" +
                "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originators \n" +
                ")  A \n" +
                "WHERE (A.sourceref IS NOT NULL) and u_key in ('%s') order by u_key desc";


    public static String GET_RANDOM_WRK_RELT_KEY_INBOUND =
           "SELECT u_key as sourceref FROM (\n" +
                   "   SELECT DISTINCT\n"+
                   "     NULLIF(concat(concat(CAST(relations.sourceref AS varchar), split_part(relations.relationtype, ' | ', 1)), CAST(relations.projectno AS varchar)), '') u_key\n"+
                   "   , NULLIF(relations.sourceref, '') parentref\n"+
                   "   , NULLIF(relations.projectno, '') childref\n"+
                   "   , NULLIF(code.ephcode, '') relationtyperef\n"+
                   "   , date_parse(NULLIF(relations.metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n"+
                   "   , 'N' dq_err\n"+
                   "   FROM\n"+
                   "     ((("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_relations relations\n"+
                   "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".relationtypecode code ON (split_part(relations.relationtype, ' | ', 1) = code.ppmcode))\n"+
                   "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily parent ON ((relations.sourceref = parent.sourceref) AND (relations.sourceref = parent.workmasterprojectno)))\n"+
                   "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily child ON ((relations.projectno = child.sourceref) AND (relations.projectno = child.workmasterprojectno)))\n"+
                   "UNION ALL    SELECT DISTINCT\n"+
                   "     concat(CAST(content.seriesid AS varchar), 'CON', CAST(content.sourceref AS varchar)) u_key\n"+
                   "   , content.seriesid parentref\n"+
                   "   , content.sourceref childref\n"+
                   "   , 'CON' relationtyperef\n"+
                   "   , date_parse(NULLIF(content.metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n"+
                   "   , 'N' dq_err\n"+
                   "   FROM\n"+
                   "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n"+
                   "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily family ON ((content.sourceref = family.sourceref) AND (content.sourceref = family.workmasterprojectno)))\n"+
                   ")  A\n"+
                   "WHERE ((((A.parentref IS NOT NULL) AND (A.parentref <> '')) AND (A.childref IS NOT NULL)) AND (A.relationtyperef IS NOT NULL))order by rand() limit %s";

    public static String GET_WORK_RELT_INBOUND_DATA =
            "select " +
                    "u_key as UKEY \n" +
                    ",parentref as PARENTREF \n" +
                    ",childref as CHILDREF \n" +
                    ",relationtyperef as RELATIONTYPEREF \n" +
                    ",dq_err as DQ_ERR from( \n" +
                    "   SELECT DISTINCT\n"+
                    "     NULLIF(concat(concat(CAST(relations.sourceref AS varchar), split_part(relations.relationtype, ' | ', 1)), CAST(relations.projectno AS varchar)), '') u_key\n"+
                    "   , NULLIF(relations.sourceref, '') parentref\n"+
                    "   , NULLIF(relations.projectno, '') childref\n"+
                    "   , NULLIF(code.ephcode, '') relationtyperef\n"+
                    "   , date_parse(NULLIF(relations.metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n"+
                    "   , 'N' dq_err\n"+
                    "   FROM\n"+
                    "     ((("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_relations relations\n"+
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".relationtypecode code ON (split_part(relations.relationtype, ' | ', 1) = code.ppmcode))\n"+
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily parent ON ((relations.sourceref = parent.sourceref) AND (relations.sourceref = parent.workmasterprojectno)))\n"+
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily child ON ((relations.projectno = child.sourceref) AND (relations.projectno = child.workmasterprojectno)))\n"+
                    "UNION ALL    SELECT DISTINCT\n"+
                    "     concat(CAST(content.seriesid AS varchar), 'CON', CAST(content.sourceref AS varchar)) u_key\n"+
                    "   , content.seriesid parentref\n"+
                    "   , content.sourceref childref\n"+
                    "   , 'CON' relationtyperef\n"+
                    "   , date_parse(NULLIF(content.metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n"+
                    "   , 'N' dq_err\n"+
                    "   FROM\n"+
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n"+
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily family ON ((content.sourceref = family.sourceref) AND (content.sourceref = family.workmasterprojectno)))\n"+
                    ")  A\n"+
                    "WHERE ((((A.parentref IS NOT NULL) AND (A.parentref <> '')) AND (A.childref IS NOT NULL)) AND (A.relationtyperef IS NOT NULL)) and u_key in ('%s') order by u_key desc";



    public static String GET_RANDOM_ACCPROD_KEY_CURRENT =
            "SELECT u_key as u_key \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_current_v \n" +
                    " order by rand() limit %s";

    public static String GET_RANDOM_MANIF_IDENT_KEY_INBOUND =
            "select u_key as sourceref from(\n" +
                    "SELECT A.*\n" +
                    ", sourceref||identifier||identifier_type as u_key FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(sourceref,'') sourceref\n" +
                    "   , NULLIF(isbn13,'') identifier\n" +
                    "   , 'ISBN' identifier_type\n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product\n" +
                    "   WHERE (isbn13 <> '')\n" +
                   /* "UNION ALL    SELECT\n" +
                    "     NULLIF(sourceref,'') sourceref\n" +
                    "   , NULLIF(seriesissn,'') identifier\n" +
                    "   , 'ISSN' identifier_type\n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content\n" +
                    "   WHERE (seriesissn <> '')\n" +*/
                    ")A WHERE A.sourceref is not null and A.identifier is not null) order by rand() limit %s";

    public static String GET_RANDOM_WORK_IDENT_KEY_INBOUND =
            "WITH\n" +
                    "  work_id AS (\n" +
                    "   SELECT DISTINCT workmasterprojectno\n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily\n" +
                    ") \n" +
                    "select u_key as sourceref from( \n" +
                    "SELECT\n" +
                    "  A.*\n" +
                    ", sourceref||identifier||identifier_type as u_key\n" +
                    "FROM\n" +
                    "  (\n" +
                    "   SELECT\n" +
                    "     NULLIF(sourceref, '') sourceref\n" +
                    "   , NULLIF(piidack, '') identifier\n" +
                    "   , 'DAC-K' identifier_type\n" +
                    "   FROM\n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
                    "   INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                    "   WHERE (piidack <> '')\n" +
                    "UNION ALL SELECT DISTINCT\n" +
                    "     NULLIF(seriesid, '') sourceref\n" +
                    "   , NULLIF(seriesissn, '') identifier\n" +
                    "   , 'ISSN-L' identifier_type\n" +
                    " from ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
                    " INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                    " WHERE (seriesissn <> '')" +
                    " UNION ALL SELECT" +
                    "     NULLIF(sourceref,'') sourceref\n" +
                    "   , NULLIF(orderno,'') identifier\n" +
                    "   , 'PPM-PART' identifier_type\n" +
                    "   FROM\n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                    "   INNER JOIN work_id ON (product.sourceref = work_id.workmasterprojectno))\n" +
                    "   WHERE (orderno <> '')\n" +
                    ")  A\n" +
                    "WHERE ((A.sourceref IS NOT NULL) AND (A.identifier IS NOT NULL))) order by rand() limit %s";

    public static String GET_WORK_IDENT_INBOUND_DATA =
              "WITH \n" +
                      "  work_id AS (\n" +
                      "   SELECT DISTINCT \"workmasterprojectno\"\n" +
                      "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily \n" +
                      ") \n" +
                      "select \n" +
                      "sourceref as SOURCEREF \n" +
                      ",u_key as UKEY \n" +
                      ",identifier as IDENTIFIER \n" +
                      ",identifier_type as IDENTIFIERTYPE \n" +
                      "from( \n" +
                      "SELECT\n" +
                      "  A.*\n" +
                      ", sourceref||identifier||identifier_type as u_key\n" +
                      "FROM\n" +
                      "  (\n" +
                      "   SELECT\n" +
                      "     NULLIF(sourceref, '') sourceref\n" +
                      "   , NULLIF(piidack, '') identifier\n" +
                      "   , 'DAC-K' identifier_type\n" +
                      "   FROM\n" +
                      "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
                      "   INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                      "   WHERE (piidack <> '')\n" +
                      "UNION ALL SELECT DISTINCT\n" +
                      "     NULLIF(seriesid, '') sourceref\n" +
                      "   , NULLIF(seriesissn, '') identifier\n" +
                      "   , 'ISSN-L' identifier_type\n" +
                      " from ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
                      " INNER JOIN work_id ON (content.sourceref = work_id.workmasterprojectno))\n" +
                      " WHERE (seriesissn <> '')" +
                      " UNION ALL SELECT" +
                      "     NULLIF(sourceref,'') sourceref\n" +
                      "   , NULLIF(orderno,'') identifier\n" +
                      "   , 'PPM-PART' identifier_type\n" +
                      "   FROM\n" +
                      "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                      "   INNER JOIN work_id ON (product.sourceref = work_id.workmasterprojectno))\n" +
                      "   WHERE (orderno <> '')\n" +
                      ")  A\n" +
                      "WHERE ((A.sourceref IS NOT NULL) AND (A.identifier IS NOT NULL))) where u_key in ('%s') order by u_key desc";

    public static String GET_MANIF_IDENT_INBOUND_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",u_key as UKEY \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from ( \n"+
             "SELECT A.*\n" +
                     ", sourceref||identifier||identifier_type as u_key FROM (\n" +
                     "SELECT DISTINCT\n" +
                     "     NULLIF(sourceref,'') sourceref\n" +
                     "   , NULLIF(isbn13,'') identifier\n" +
                     "   , 'ISBN' identifier_type\n" +
                     "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product\n" +
                     "   WHERE (isbn13 <> '')\n" +
                 /*    "UNION ALL    SELECT\n" +
                     "     NULLIF(sourceref,'') sourceref\n" +
                     "   , NULLIF(seriesissn,'') identifier\n" +
                     "   , 'ISSN' identifier_type\n" +
                     "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content\n" +
                     "   WHERE (seriesissn <> '')\n" +*/
                     ")A WHERE A.sourceref is not null and A.identifier is not null) where u_key in ('%s') order by u_key desc";


    public static String GET_MANIF_IDENT_CURR_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",u_key as UKEY \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_identifier_current_v where u_key in ('%s') order by u_key desc";

    public static String GET_WORK_IDENT_CURR_DATA =
            "select " +
                    "sourceref as SOURCEREF \n" +
                    ",u_key as UKEY \n" +
                    ",identifier as IDENTIFIER \n" +
                    ",identifier_type as IDENTIFIERTYPE \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_identifier_current_v where u_key in ('%s') order by u_key desc";

    public static String GET_PERSON_CURR_DATA =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",u_key as UKEY" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQ_ERR " +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_current_v where u_key in \n" +
                    "('%s') order by u_key desc";


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
            "SELECT sourceref as SOURCEREF " +
                    ",u_key as UKEY\n" +
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

    public static String GET_RANDOM_PRODUCT_KEY_INBOUND =
            "SELECT u_key as sourceref \n" +
                    "FROM\n" +
                    "  (\n" +
                    "   SELECT DISTINCT\n" +
                    "     NULLIF(product.sourceref, '') sourceref\n" +
                    "   , split_part(NULLIF(product.binding, ''), ' | ', 1) bindingcode\n" +
                    "   , coalesce(product.sourceref, '') u_key \n" +
                    "   , concat(coalesce(content.title, ''), ' Purchase') name\n" +
                    "   , NULLIF(content.shorttitle, '') shorttitle\n" +
                    "   , CAST(date_parse((CASE WHEN ((publishedon = '') AND (pubdateplanned = '')) THEN CAST(null AS varchar) ELSE (CASE WHEN (publishedon = '') THEN pubdateplanned ELSE publishedon END) END), '%%d-%%b-%%Y') AS date) launchdate\n" +
                    "   , NULLIF(taxcode.ephcode, '') taxcode\n" +
                    "   , CASE WHEN product.metadeleted = 'Y' THEN 'NVP' ELSE COALESCE(NULLIF(status.eph_product_status_code, ''), 'UNK') END status\n" +
                    "   , NULLIF(product.sourceref, '') manifestationref\n" +
                    "   , NULLIF(workcontent.sourceref, '') worksource\n" +
                    "   , NULLIF(workcontent.work_type, '') work_type\n" +
                    "   , CASE WHEN separately_sale.sourceref is null then true else false END  separately_sale_indicator \n" +
                    "   , CAST(null AS boolean) trial_allowed_indicator\n" +
                    "   , CASE WHEN sales_rest.sourceref is null then false else true END restricted_sale_indicator \n" +
                    "   , CAST(null AS varchar) f_work_source_ref\n" +
                    "   , 'OOA' product_type\n" +
                    "   , 'ONE' f_revenue_model\n" +
                    "   , 'N' dq_err\n" +
                    "   FROM\n" +
                    "     "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily versionfamily ON ((product.sourceref = versionfamily.sourceref) AND (versionfamily.workmasterprojectno is not null))\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content ON (versionfamily.workmasterprojectno = content.sourceref)\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_current_v workcontent ON (versionfamily.workmasterprojectno = workcontent.sourceref)\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification ON ((product.sourceref = classification.sourceref) AND (classificationcode LIKE 'DCDFC1%%'))\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".taxcode ON (split_part(classification.value, ' | ', 1) = taxcode.ppmcode)\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".all_manifestation_statuses_v manifestationstatus ON (product.sourceref = manifestationstatus.sourceref)\n" +
                    "   LEFT JOIN (SELECT DISTINCT eph_product_status_code, product_priority FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode) status ON coalesce(nullif(manifestationstatus.ref_key_product_priority,6),nullif(manifestationstatus.delivery_status_product_priority,6),nullif(manifestationstatus.delta_status_product_priority,6),6) = status.product_priority\n" +
                    "   LEFT JOIN (\n" +
                    "      select distinct sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation where refkey in ('IPR | IPR - In Preparation (Secret)','NR | NR - No rights','NRG | NRG - No sales rights', 'CSR | CSR - CS Research')\n" +
                    "      UNION\n" +
                    "      select distinct sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product where refkey in ('IPR | IPR - In Preparation (Secret)','NR | NR - No rights','NRG | NRG - No sales rights', 'CSR | CSR - CS Research')\n" +
                    "      UNION\n" +
                    "      select distinct sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification where classificationcode like ('MANOTAVA%%')\n" +
                    "      ) sales_rest on product.sourceref = sales_rest.sourceref \n" +
                    "   LEFT JOIN (\n" +
                    "      select distinct sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation where refkey in ('NSS | NSS - Not sold separately','NSI | NSI - Non-saleable item')\n" +
                    "      UNION\n" +
                    "      select distinct sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product where refkey in ('NSS | NSS - Not sold separately','NSI | NSI - Non-saleable item')\n" +
                    "      UNION\n" +
                    "      select sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification where split_part(classificationcode,' | ',1) in ('DCADA','DCAADAUS','DCAANZ') and split_part(value,' | ',1) in ('NSS','NSI')\n" +
                    "      ) separately_sale on product.sourceref = separately_sale.sourceref \n" +
                    ")  A\n" +
                    "WHERE (A.sourceref IS NOT NULL) order by  rand() limit %s \n";

    public static String GET_PROD_INBOUND_DATA =
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
                    " FROM\n" +
                    "  (\n" +
                    "   SELECT DISTINCT\n" +
                    "     NULLIF(product.sourceref, '') sourceref\n" +
                    "   , split_part(NULLIF(product.binding, ''), ' | ', 1) bindingcode\n" +
                    "   , coalesce(product.sourceref, '') u_key \n" +
                    "   , concat(coalesce(content.title, ''), ' Purchase') name\n" +
                    "   , NULLIF(content.shorttitle, '') shorttitle\n" +
                    "   , CAST(date_parse((CASE WHEN ((publishedon = '') AND (pubdateplanned = '')) THEN CAST(null AS varchar) ELSE (CASE WHEN (publishedon = '') THEN pubdateplanned ELSE publishedon END) END), '%%d-%%b-%%Y') AS date) launchdate\n" +
                    "   , NULLIF(taxcode.ephcode, '') taxcode\n" +
                    "   , CASE WHEN product.metadeleted = 'Y' THEN 'NVP' ELSE COALESCE(NULLIF(status.eph_product_status_code, ''), 'UNK') END status\n" +
                    "   , NULLIF(product.sourceref, '') manifestationref\n" +
                    "   , NULLIF(workcontent.sourceref, '') worksource\n" +
                    "   , NULLIF(workcontent.work_type, '') work_type\n" +
                    "   , CASE WHEN separately_sale.sourceref is null then true else false END  separately_sale_indicator \n" +
                    "   , CAST(null AS boolean) trial_allowed_indicator\n" +
                    "   , CASE WHEN sales_rest.sourceref is null then false else true END restricted_sale_indicator \n" +
                    "   , CAST(null AS varchar) f_work_source_ref\n" +
                    "   , 'OOA' product_type\n" +
                    "   , 'ONE' f_revenue_model\n" +
                    "   , 'N' dq_err\n" +
                    "   FROM\n" +
                    "     "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily versionfamily ON ((product.sourceref = versionfamily.sourceref) AND (versionfamily.workmasterprojectno is not null))\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content ON (versionfamily.workmasterprojectno = content.sourceref)\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_current_v workcontent ON (versionfamily.workmasterprojectno = workcontent.sourceref)\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification ON ((product.sourceref = classification.sourceref) AND (classificationcode LIKE 'DCDFC1%%'))\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".taxcode ON (split_part(classification.value, ' | ', 1) = taxcode.ppmcode)\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".all_manifestation_statuses_v manifestationstatus ON (product.sourceref = manifestationstatus.sourceref)\n" +
                    "   LEFT JOIN (SELECT DISTINCT eph_product_status_code, product_priority FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode) status ON coalesce(nullif(manifestationstatus.ref_key_product_priority,6),nullif(manifestationstatus.delivery_status_product_priority,6),nullif(manifestationstatus.delta_status_product_priority,6),6) = status.product_priority\n" +
                    "   LEFT JOIN (\n" +
                    "      select distinct sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation where refkey in ('IPR | IPR - In Preparation (Secret)','NR | NR - No rights','NRG | NRG - No sales rights', 'CSR | CSR - CS Research')\n" +
                    "      UNION\n" +
                    "      select distinct sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product where refkey in ('IPR | IPR - In Preparation (Secret)','NR | NR - No rights','NRG | NRG - No sales rights', 'CSR | CSR - CS Research')\n" +
                    "      UNION\n" +
                    "      select distinct sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification where classificationcode like ('MANOTAVA%%')\n" +
                    "      ) sales_rest on product.sourceref = sales_rest.sourceref \n" +
                    "   LEFT JOIN (\n" +
                    "      select distinct sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation where refkey in ('NSS | NSS - Not sold separately','NSI | NSI - Non-saleable item')\n" +
                    "      UNION\n" +
                    "      select distinct sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product where refkey in ('NSS | NSS - Not sold separately','NSI | NSI - Non-saleable item')\n" +
                    "      UNION\n" +
                    "      select sourceref from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification where split_part(classificationcode,' | ',1) in ('DCADA','DCAADAUS','DCAANZ') and split_part(value,' | ',1) in ('NSS','NSI')\n" +
                    "      ) separately_sale on product.sourceref = separately_sale.sourceref \n" +
                    ")  A\n" +
                    "WHERE (A.sourceref IS NOT NULL) and u_key in ('%s') order by u_key desc";

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
                    "u_key as UKEY \n" +
                    ",sourceref as SOURCEREF \n" +
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
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_current_v where u_key in ('%s') order by u_key desc";


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
                    "('%s') order by u_key desc";

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
                    "and u_key in ('%s') \n" +
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
                    "and u_key in ('%s') \n" +
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
                    "            coalesce(crr.u_key, 'na') <> coalesce(prev.u_key, 'na') or\n" +
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
                    "            coalesce(crr.u_key, 'na') <> coalesce(prev.u_key, 'na') or\n" +
                    "            coalesce (crr.firstname, 'na') <> coalesce (prev.firstname, 'na') or\n" +
                    "            coalesce (crr.familyname, 'na') <> coalesce (prev.familyname, 'na') or\n" +
                    "            coalesce (crr.peoplehub_id, 'na') <> coalesce (prev.peoplehub_id, 'na') or\n" +
                    "            coalesce (crr.email_address, 'na') <> coalesce (prev.email_address, 'na') or\n" +
                    "            coalesce (crr.dq_err, 'na') <> coalesce (prev.dq_err, 'na')))where u_key in ('%s') order by u_key desc ";


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
                    "('%s') order by u_key desc";

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
                    "and u_key in ('%s') \n" +
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
                    "select d.u_key,d.sourceref,d.title,d.intereditionflag,d.firstpublisheddate,d.binding,d.manifestation_type,(CASE WHEN (delta_mode = 'D') THEN 'NVM' ELSE d.status END) status,d.work_id,d.last_pub_date,d.dq_err \n" +
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
                    "c.product_type,c.f_revenue_model,c.dq_err from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_product_excl_delta as c union all \n" +
                    "select d.sourceref,d.bindingcode,d.u_key,d.name,d.shorttitle,d.launchdate,d.taxcode,(CASE WHEN (delta_mode = 'D') THEN 'NVP' ELSE d.status END) status,d.manifestationref, \n" +
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
                    ",d.work_type,(CASE WHEN (delta_mode = 'D') THEN 'NVW' ELSE d.statuscode END) statuscode,d.imprintcode,d.opco,d.resp_centre,d.te_opco,\n" +
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
                    " where u_key in ('%s') order by u_key desc";

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
                    "where u_key in ('%s') \n" +
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
                    " where u_key in ('%s') order by u_key desc";

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
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_MANIF_STATUS_KEY_INBOUND =
            "select sourceref from(\n" +
                    "select \n" +
                    "       sourceref \n" +
                    "     , min(ref_key_product_priority) ref_key_product_priority \n" +
                    "     , min(delivery_status_product_priority) delivery_status_product_priority \n" +
                    "     , min(delta_status_product_priority) delta_status_product_priority \n" +
                    "     , min(ref_key_manifestation_priority) ref_key_manifestation_priority \n" +
                    "     , min(delivery_status_manifestation_priority) delivery_status_manifestation_priority \n" +
                    "     , min(delta_status_manifestation_priority) delta_status_manifestation_priority \n" +
                    " from ( \n" +
                    "  select \n" +
                    "       sourceref \n" +
                    "     , min(product_priority) ref_key_product_priority \n" +
                    "     , min(manifestation_priority) ref_key_manifestation_priority \n" +
                    "     , cast(null as integer) delivery_status_product_priority \n" +
                    "     , cast(null as integer) delivery_status_manifestation_priority \n" +
                    "     , cast(null as integer) delta_status_product_priority \n" +
                    "     , cast(null as integer) delta_status_manifestation_priority \n" +
                    "  from \n" +
                    "    ( \n" +
                    "     select sourceref, split_part(refkey,' | ',1) refkey, product_priority, manifestation_priority \n" +
                    "     from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product\n" +
                    "     left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode on split_part(refkey,' | ',1) = ppm_code\n" +
                    "     union\n" +
                    "     select sourceref, split_part(refkey,' | ',1) refkey, product_priority, manifestation_priority\n" +
                    "     from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation\n" +
                    "     left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode on split_part(refkey,' | ',1) = ppm_code\n" +
                    "    )\n" +
                    "  group by sourceref\n" +
                    "  union all\n" +
                    "  select\n" +
                    "       sourceref\n" +
                    "     , cast(null as integer) ref_key_product_priority\n" +
                    "     , cast(null as integer) ref_key_manifestation_priority\n" +
                    "     , min(product_priority) delivery_status_product_priority\n" +
                    "     , min(manifestation_priority) delivery_status_manifestation_priority\n" +
                    "     , cast(null as integer) delta_status_product_priority\n" +
                    "     , cast(null as integer) delta_status_manifestation_priority\n" +
                    "  from\n" +
                    "    (\n" +
                    "     select sourceref, split_part(deliverystatus,' | ',1) delivery_status, product_priority, manifestation_priority\n" +
                    "     from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product\n" +
                    "     left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode on split_part(deliverystatus,' | ',1) = ppm_code\n" +
                    "     union\n" +
                    "     select sourceref, split_part(status,' | ',1) delivery_status, product_priority, manifestation_priority\n" +
                    "     from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation\n" +
                    "     left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode on split_part(status,' | ',1) = ppm_code\n" +
                    "    )\n" +
                    "  group by sourceref\n" +
                    "  union all\n" +
                    "  select\n" +
                    "       sourceref\n" +
                    "     , cast(null as integer) ref_key_product_priority\n" +
                    "     , cast(null as integer) ref_key_manifestation_priority\n" +
                    "     , cast(null as integer) delivery_status_product_priority\n" +
                    "     , cast(null as integer) delivery_status_manifestation_priority\n" +
                    "     , min(product_priority) delta_status_product_priority\n" +
                    "     , min(manifestation_priority) delta_status_manifestation_priority\n" +
                    "  from\n" +
                    "    (\n" +
                    "     select sourceref, split_part(value,' | ',1) delta_status, product_priority, manifestation_priority\n" +
                    "     from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification\n" +
                    "     left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode on split_part(value,' | ',1) = ppm_code\n" +
                    "     where split_part(classificationcode,' | ',1) in ('DCADA','DCAADAUS','DCAANZ')\n" +
                    "    )\n" +
                    "  group by sourceref\n" +
                    " )\n" +
                    "group by sourceref\n" +
                    ") order by rand() limit %s";


    public static String GET_RANDOM_MANIF_PUBDATES_KEY_INBOUND =
      "select sourceref from(\n" +
              "select \n" +
              "a.sourceref, \n" +
              "versionfamily.workmasterprojectno, \n" +
              "min(publishedondate) min_actual_pubdate, \n" +
              "min(pubdateplanneddate) min_planned_pubdate\n" +
              "from (\n" +
              "    select content.ownership\n" +
              "         , content.sourceref\n" +
              "         , cast(date_parse(nullif(product.publishedon, ''), '%%d-%%b-%%Y') as date) publishedondate\n" +
              "         , cast(date_parse(nullif(product.pubdateplanned, ''), '%%d-%%b-%%Y') as date) pubdateplanneddate\n" +
              "    from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
              "    inner join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content on product.sourceref = content.sourceref\n" +
              "    union all \n" +
              "    select location.warehouse\n" +
              "         , location.sourceref\n" +
              "         , cast(date_parse(nullif(location.pubdateactual, ''), '%%d-%%b-%%Y') as date) publishedondate\n" +
              "         , cast(date_parse(nullif(location.plannedpubdate, ''), '%%d-%%b-%%Y') as date) pubdateplanneddate\n" +
              "    from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation location\n" +
              "    ) a\n" +
              "inner join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily versionfamily on a.sourceref = versionfamily.sourceref \n" +
              "and nullif(versionfamily.workmasterprojectno,'')is not null\n" +
              "group by a.sourceref, versionfamily.workmasterprojectno) order by rand() limit %s \n" ;


    public static String GET_ALL_MANIF_STATUS_INBOUND_DATA =
            "select sourceref as SOURCEREF \n" +
                    ",ref_key_product_priority as REFKEYPRODPRIORITY \n" +
                    ",delivery_status_product_priority as DELIVERYSTATUSPRODPRIORITY \n" +
                    ",delta_status_product_priority as DELTASTATUSPRODPRIORITY \n" +
                    ",ref_key_manifestation_priority as REFKEYMANIFPRIORITY \n" +
                    ",delivery_status_manifestation_priority as DELIVERYSTATUSMANIFPRIORITY \n" +
                    ",delta_status_manifestation_priority as DELTASTATUSMANIFPRIORITY \n" +
                    " from(\n" +
                    "select \n" +
                    "       sourceref \n" +
                    "     , min(ref_key_product_priority) ref_key_product_priority \n" +
                    "     , min(delivery_status_product_priority) delivery_status_product_priority \n" +
                    "     , min(delta_status_product_priority) delta_status_product_priority \n" +
                    "     , min(ref_key_manifestation_priority) ref_key_manifestation_priority \n" +
                    "     , min(delivery_status_manifestation_priority) delivery_status_manifestation_priority \n" +
                    "     , min(delta_status_manifestation_priority) delta_status_manifestation_priority \n" +
                    " from ( \n" +
                    "  select \n" +
                    "       sourceref \n" +
                    "     , min(product_priority) ref_key_product_priority \n" +
                    "     , min(manifestation_priority) ref_key_manifestation_priority \n" +
                    "     , cast(null as integer) delivery_status_product_priority \n" +
                    "     , cast(null as integer) delivery_status_manifestation_priority \n" +
                    "     , cast(null as integer) delta_status_product_priority \n" +
                    "     , cast(null as integer) delta_status_manifestation_priority \n" +
                    "  from \n" +
                    "    ( \n" +
                    "     select sourceref, split_part(refkey,' | ',1) refkey, product_priority, manifestation_priority \n" +
                    "     from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product\n" +
                    "     left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode on split_part(refkey,' | ',1) = ppm_code\n" +
                    "     union\n" +
                    "     select sourceref, split_part(refkey,' | ',1) refkey, product_priority, manifestation_priority\n" +
                    "     from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation\n" +
                    "     left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode on split_part(refkey,' | ',1) = ppm_code\n" +
                    "    )\n" +
                    "  group by sourceref\n" +
                    "  union all\n" +
                    "  select\n" +
                    "       sourceref\n" +
                    "     , cast(null as integer) ref_key_product_priority\n" +
                    "     , cast(null as integer) ref_key_manifestation_priority\n" +
                    "     , min(product_priority) delivery_status_product_priority\n" +
                    "     , min(manifestation_priority) delivery_status_manifestation_priority\n" +
                    "     , cast(null as integer) delta_status_product_priority\n" +
                    "     , cast(null as integer) delta_status_manifestation_priority\n" +
                    "  from\n" +
                    "    (\n" +
                    "     select sourceref, split_part(deliverystatus,' | ',1) delivery_status, product_priority, manifestation_priority\n" +
                    "     from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product\n" +
                    "     left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode on split_part(deliverystatus,' | ',1) = ppm_code\n" +
                    "     union\n" +
                    "     select sourceref, split_part(status,' | ',1) delivery_status, product_priority, manifestation_priority\n" +
                    "     from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation\n" +
                    "     left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode on split_part(status,' | ',1) = ppm_code\n" +
                    "    )\n" +
                    "  group by sourceref\n" +
                    "  union all\n" +
                    "  select\n" +
                    "       sourceref\n" +
                    "     , cast(null as integer) ref_key_product_priority\n" +
                    "     , cast(null as integer) ref_key_manifestation_priority\n" +
                    "     , cast(null as integer) delivery_status_product_priority\n" +
                    "     , cast(null as integer) delivery_status_manifestation_priority\n" +
                    "     , min(product_priority) delta_status_product_priority\n" +
                    "     , min(manifestation_priority) delta_status_manifestation_priority\n" +
                    "  from\n" +
                    "    (\n" +
                    "     select sourceref, split_part(value,' | ',1) delta_status, product_priority, manifestation_priority\n" +
                    "     from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification\n" +
                    "     left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode on split_part(value,' | ',1) = ppm_code\n" +
                    "     where split_part(classificationcode,' | ',1) in ('DCADA','DCAADAUS','DCAANZ')\n" +
                    "    )\n" +
                    "  group by sourceref\n" +
                    " )\n" +
                    "group by sourceref\n" +
                    ") where sourceref in ('%s') order by sourceref desc";

    public static String GET_ALL_MANIF_PUBDATES_INBOUND_DATA =
    "select sourceref as SOURCEREF " +
            ",workmasterprojectno as WORKMASTERPROJECTNO" +
            ",min_actual_pubdate as MINACTUALPUBDATE" +
            ",min_planned_pubdate as MINPLANNEDPUBDATE" +
            " from(\n" +
            "select \n" +
            "a.sourceref, \n" +
            "versionfamily.workmasterprojectno, \n" +
            "min(publishedondate) min_actual_pubdate, \n" +
            "min(pubdateplanneddate) min_planned_pubdate\n" +
            "from (\n" +
            "    select content.ownership\n" +
            "         , content.sourceref\n" +
            "         , cast(date_parse(nullif(product.publishedon, ''), '%%d-%%b-%%Y') as date) publishedondate\n" +
            "         , cast(date_parse(nullif(product.pubdateplanned, ''), '%%d-%%b-%%Y') as date) pubdateplanneddate\n" +
            "    from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
            "    inner join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content on product.sourceref = content.sourceref\n" +
            "    union all \n" +
            "    select location.warehouse\n" +
            "         , location.sourceref\n" +
            "         , cast(date_parse(nullif(location.pubdateactual, ''), '%%d-%%b-%%Y') as date) publishedondate\n" +
            "         , cast(date_parse(nullif(location.plannedpubdate, ''), '%%d-%%b-%%Y') as date) pubdateplanneddate\n" +
            "    from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation location\n" +
            "    ) a\n" +
            "inner join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily versionfamily on a.sourceref = versionfamily.sourceref \n" +
            "and nullif(versionfamily.workmasterprojectno,'')is not null\n" +
            "group by a.sourceref, versionfamily.workmasterprojectno) where sourceref in ('%s') order by sourceref desc";

    public static String GET_MANIF_STATUSES_DATA =
            "select sourceref as SOURCEREF \n" +
                    ",ref_key_product_priority as REFKEYPRODPRIORITY \n" +
                    ",delivery_status_product_priority as DELIVERYSTATUSPRODPRIORITY \n" +
                    ",delta_status_product_priority as DELTASTATUSPRODPRIORITY \n" +
                    ",ref_key_manifestation_priority as REFKEYMANIFPRIORITY \n" +
                    ",delivery_status_manifestation_priority as DELIVERYSTATUSMANIFPRIORITY \n" +
                    ",delta_status_manifestation_priority as DELTASTATUSMANIFPRIORITY \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".all_manifestation_statuses_v where sourceref in ('%s') order by sourceref desc";

    public static String GET_MANIF_PUBDATES_DATA =
            "select sourceref as SOURCEREF " +
                    ",workmasterprojectno as WORKMASTERPROJECTNO" +
                    ",min_actual_pubdate as MINACTUALPUBDATE" +
                    ",min_planned_pubdate as MINPLANNEDPUBDATE" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".all_manifestation_pubdates_v where sourceref in ('%s') order by sourceref desc";














}






