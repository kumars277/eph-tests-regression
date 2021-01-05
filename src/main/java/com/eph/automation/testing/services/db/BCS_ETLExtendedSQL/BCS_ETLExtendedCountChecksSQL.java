package com.eph.automation.testing.services.db.BCS_ETLExtendedSQL;


import com.eph.automation.testing.services.db.BCS_ETLCoreSQL.GetBCS_ETLCoreDLDBUser;

public class BCS_ETLExtendedCountChecksSQL {

    public static String GET_BCS_EXTENDED_AVAILABILITY_EXTENDED_CURR_COUNT="select count(*) as Target_Count from "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_current_v";

    public static String GET_BCS_EXTENDED_MANIFESTATION_EXTENDED_CURR_COUNT="select count(*) as Target_Count from " + GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_current_v";

    public static String GET_BCS_EXTENDED_PAGE_COUNT_EXTENDED_CURR_COUNT="select count(*) as Target_Count from " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_current_v ";

    public static String GET_BCS_EXTENDED_URL_EXTENDED_CURR_COUNT="select count(*) as Target_Count from " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_current_v";

    public static String GET_BCS_EXTENDED_WORK_EXTENDED_CURR_COUNT="select count(*) as Target_Count from " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_current_v";

    public static String GET_BCS_EXTENDED_WORK_SUBJECT_AREA_EXTENDED_CURR_COUNT="select count(*) as Target_Count from " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_current_v";

    public static String GET_BCS_EXTENDED_MANIFESTATION_RESTRICTIONS_EXTENDED_CURR_COUNT="select count(*) as Target_Count from " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_current_v";

    public static String GET_BCS_EXTENDED_PRODUCT_PRICES_EXTENDED_CURR_COUNT="select count(*) as Target_Count from " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_current_v";

    public static String GET_BCS_EXTENDED_WORK_PERSON_ROLE_EXTENDED_CURR_COUNT="select count(*) as Target_Count from " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_current_v";

    public static String GET_AVAILABILITY_INBOUND_CURRENT_COUNT= "select count(*) as Source_Count from (\n" +
            "SELECT distinct cr.epr eprId, concat(A.sourceref,A.application) u_key, cr.product_type productType, A.* FROM ( \n" +
            "SELECT \n" +
            "     NULLIF(p.sourceref,'') sourceref \n" +
            "   , date_parse(NULLIF(p.metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon \n" +
            "   , 'Delta Books' application \n" +
            "   , split_part(NULLIF(duk.value,''), ' | ', 1) deltaanswercodeuk \n" +
            "   , split_part(NULLIF(dus.value,''), ' | ', 1) deltaanswercodeus \n" +
            "   , split_part (NULLIF(dan.value,''), ' | ', 1) anzpubstatus \n" +
            "   , cast(null as date) pubdateactual \n" +
            "   , cast(null as varchar) status \n" +
            "   , CASE WHEN p.metadeleted = 'Y' THEN true else false END metadeleted \n" +
            "   FROM\n" +
            "     ((("+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product p \n" +
            "   LEFT JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification duk ON ((p.sourceref = duk.sourceref) AND (split_part(duk.classificationcode, ' | ', 1) = 'DCADA')))\n" +
            "   LEFT JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification dus ON ((p.sourceref = dus.sourceref) AND (split_part(dus.classificationcode, ' | ', 1) = 'DCAADAUS')))\n" +
            "   LEFT JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification dan ON ((p.sourceref = dan.sourceref) AND (split_part(dan.classificationcode, ' | ', 1) = 'DCAANZ')))\n" +
            "UNION    SELECT \n" +
            "     NULLIF(sourceref,'') sourceref \n" +
            "   , date_parse(NULLIF(metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon \n" +
            "   , NULLIF(locationcode.ephcode,'') application \n" +
            "   , null deltaanswercodeuk \n" +
            "   , null deltaanswercodeus \n" +
            "   , null anzpubstatus \n" +
            "   , cast(date_parse(coalesce(NULLIF(pubdateactual,''),NULLIF(plannedpubdate,'')),'%d-%b-%Y') as date) pubdateactual \n" +
            "   , split_part (NULLIF(status,''), ' | ', 1) status \n" +
            "   , CASE WHEN metadeleted = 'Y' THEN true else false END metadeleted \n" +
            "   FROM\n" +
            "     "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation \n" +
            "   INNER JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".locationcode ON (split_part(sublocation.warehouse, ' | ', 1) = locationcode.ppmcode)\n" +
            "   WHERE NULLIF(trim(sublocation.refkey,' '),'') IS NOT NULL\n" +
            "UNION SELECT\n" +
            "     NULLIF(product.sourceref,'') sourceref \n" +
            "   , date_parse(NULLIF(product.metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon \n" +
            "   , NULLIF(locationcode.ephcode,'') application \n" +
            "   , null deltaanswercodeuk \n" +
            "   , null deltaanswercodeus \n" +
            "   , null anzpubstatus \n" +
            "   , cast(date_parse(coalesce(NULLIF(product.publishedon,''),NULLIF(product.pubdateplanned,'')),'%d-%b-%Y') as date) pubdateactual \n" +
            "   , split_part (NULLIF(product.deliverystatus,''), ' | ', 1) status \n" +
            "   , CASE WHEN product.metadeleted = 'Y' THEN true else false END metadeleted \n" +
            "   FROM \n" +
            "     "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
            "   INNER JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content on product.sourceref = content.sourceref \n" +
            "   INNER JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".locationcode ON (content.ownership = locationcode.ppmcode)\n" +
            "   WHERE NULLIF(trim(product.refkey,' '),'') IS NOT NULL  \n" +
            ")A \n" +
            "INNER JOIN " + GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON \n" +
            "A.sourceref = cr.identifier AND \n" +
            "cr.identifier_type = 'external_reference' AND \n" +
            "cr.record_level = 'Product'\n" +
            "WHERE A.metadeleted = FALSE )\n";


    public static String GET_MANIFESTATION_INBOUND_CURRENT_COUNT="select count(*) as Source_Count from (\n" +
            "SELECT distinct cr.epr eprId, A.sourceref u_key, cr.manifestation_type, A.* FROM ( \n" +
            "SELECT\n" +
            "     NULLIF(m.sourceref,'') sourceref \n" +
            "   , date_parse(NULLIF(m.metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon \n" +
            "   , CASE WHEN m.metadeleted = 'Y' THEN true else false END metadeleted \n" +
            "   , (CASE WHEN (NULLIF(ukt.classificationcode,'') IS NULL) THEN false ELSE true END) uktextbookind \n" +
            "   , (CASE WHEN (NULLIF(ust.classificationcode,'') IS NULL) THEN false ELSE true END) ustextbookind \n" +
            "   , split_part(NULLIF(usd.value,''), ' | ', 1) usdiscountcode\n" +
            "   , split_part(NULLIF(usd.value,''), ' | ', 2) usdiscountname \n" +
            "   , split_part(NULLIF(ukd.value,''), ' | ', 1) emeadiscountcode \n" +
            "   , split_part(NULLIF(ukd.value,''), ' | ', 2) emeadiscountname \n" +
            "   , coalesce(split_part(NULLIF(p.trimsize,'') , ' | ', 1),NULLIF(p.trimother,'')) trimsize \n" +
            "   , cast(NULLIF(p.weight,'') as double) weight \n" +
            "   , split_part(NULLIF(c.value,'') , ' | ', 1) commcode\n" +
            "   , CAST(NULL AS varchar) journalProdSiteCode \n" +
            "   , CAST(NULL AS varchar) journalIssueTrimSize \n" +
            "   , CAST(NULL AS varchar) warReference \n" +
            "   FROM\n" +
            "     ((((((("+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product m\n" +
            "   LEFT JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_production p ON (m.sourceref = p.sourceref))\n" +
            "   LEFT JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification ukt ON ((m.sourceref = ukt.sourceref) AND (ukt.classificationcode LIKE 'MAUKT%')))\n" +
            "   LEFT JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification ust ON ((m.sourceref = ust.sourceref) AND (ust.classificationcode LIKE 'MAUST%')))\n" +
            "   LEFT JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification usd ON ((m.sourceref = usd.sourceref) AND (usd.classificationcode LIKE 'MADISC %')))\n" +
            "   LEFT JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification ukd ON ((m.sourceref = ukd.sourceref) AND (ukd.classificationcode LIKE 'MADISCEMEA%')))\n" +
            "   LEFT JOIN "+ GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((m.sourceref = c.sourceref) AND (c.classificationcode LIKE 'DCDFC1%')))\n" +
            ")\n" +
            ")A\n" +
            "INNER JOIN " + GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
            "A.sourceref = cr.identifier AND \n" +
            "cr.identifier_type = 'external_reference' AND \n" +
            "cr.record_level = 'Manifestation'\n" +
            "WHERE A.metadeleted = false)\n";

    public static String GET_PAGE_COUNT_INBOUND_CURRENT_COUNT=
            "select count(*) as Source_Count from (\n" +
            "   \n" +
            "   SELECT distinct cr.epr eprId, concat(A.sourceref,A.pagecounttypecode) u_key, cr.manifestation_type, A.* FROM (\n" +
            "SELECT DISTINCT\n" +
            "     NULLIF(sourceref,'') sourceref \n" +
            "   , date_parse(NULLIF(metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon \n" +
            "   , CASE WHEN metadeleted = 'Y' then true else false END metadeleted \n" +
            "   , 'ROMAN' pagecounttypecode \n" +
            "   , 'Roman' pagecounttypename \n" +
            "   , CAST((CASE WHEN (pagesroman = '') THEN '0' ELSE pagesroman END) AS integer) pagecount \n" +
            "   FROM\n" +
            "     " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_production\n" +
            "UNION    SELECT\n" +
            "     NULLIF(sourceref,'') sourceref \n" +
            "   , date_parse(NULLIF(metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon \n" +
            "   , CASE WHEN metadeleted = 'Y' then true else false END metadeleted \n" +
            "   , 'ARABIC' pagecounttypecode \n" +
            "   , 'Arabic' pagecounttypename \n" +
            "   , CAST((CASE WHEN (pagesarabic = '') THEN '0' ELSE pagesarabic END) AS integer) pagecount \n" +
            "   FROM \n" +
            "     " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_production\n" +
            "UNION    SELECT\n" +
            "     NULLIF(sourceref,'') sourceref \n" +
            "   , date_parse(NULLIF(metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon \n" +
            "   , CASE WHEN metadeleted = 'Y' then true else false END metadeleted\n" +
            "   , 'TOTAL' pagecounttypecode \n" +
            "   , 'Total' pagecounttypename \n" +
            "   , (CAST((CASE WHEN (pagesroman = '') THEN '0' ELSE pagesroman END) AS integer) + CAST((CASE WHEN (pagesarabic = '') THEN '0' ELSE pagesarabic END) AS integer)) pagecount \n" +
            "   FROM\n" +
            "     " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_production\n" +
            ")A\n" +
            "INNER JOIN " +GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
            "A.sourceref = cr.identifier AND \n" +
            "cr.identifier_type = 'external_reference' AND \n" +
            "cr.record_level = 'Manifestation'\n" +
            "WHERE \"A\".metadeleted = FALSE \n" +
            "\n" +
            ")\n";

    public static String GET_URL_INBOUND_CURRENT_COUNT=
            "select count(*) as Source_Count from (\n" +
            "SELECT distinct cr.epr eprId, concat(A.sourceref,A.source) u_key, cr.work_type, A.* FROM (\n" +
            "SELECT DISTINCT\n" +
            "     NULLIF(sourceref,'') sourceref \n" +
            "   , date_parse(NULLIF(metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon \n" +
            "   , NULLIF(urltypecode.ephcode,'') urltypecode \n" +
            "   , NULLIF(urltypecode.ephdescription,'') urltypename \n" +
            "   , NULLIF(source,'') source\n" +
            "   , NULLIF(name,'') name \n" +
            "   , CASE WHEN metadeleted = 'Y' then true else false END metadeleted \n" +
            "   FROM\n" +
            "     (" +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_extobject\n" +
            "   INNER JOIN " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".urltypecode ON (split_part(object, ' | ', 1) = ppmcode))\n" +
            ")A\n" +
            "INNER JOIN " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily f ON A.sourceref = f.sourceref and A.sourceref = f.workmasterprojectno \n" +
            "INNER JOIN " +GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
            "f.workmasterprojectno = cr.identifier AND \n" +
            "cr.identifier_type = 'external_reference' AND \n" +
            "cr.record_level = 'Work'\n" +
            "WHERE A.metadeleted = FALSE \n" +
            ")";

    public static String GET_WORK_INBOUND_CURRENT_COUNT="select count(*) as Source_Count from (\n" +
            "\n" +
            "SELECT distinct cr.epr eprId, A.sourceref u_key, cr.work_type, A.* FROM ( \n" +
            "SELECT DISTINCT\n" +
            "     NULLIF(c.sourceref,'') sourceref \n" +
            "   , NULLIF(c.companygroup,'') companygroup \n" +
            "   , date_parse(NULLIF(c.metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon \n" +
            "   , CASE WHEN c.metadeleted = 'Y' then true else false END metadeleted \n" +
            "   , concat('http://secure-ecsd.elsevier.com/covers/80/Tango2/large/',v.workmasterisbn,'.jpg') imagefileref \n" +
            "   , NULLIF(v.workmasterisbn,'') workmasterisbn \n" +
            "   , NULLIF(t.textreftrade,'') textreftrade \n" +
            "   , NULLIF(f.bebullcpy,'') features \n" +
            "   , NULLIF(f.semsbka,'') awards \n" +
            "   , NULLIF(f.betoclng,'') toc_long \n" +
            "   , NULLIF(f.betocsht,'') toc_short \n" +
            "   , NULLIF(f.semktaud,'') audience \n" +
            "   , NULLIF(f.beaubyl,'') authorbyline \n" +
            "   , NULLIF(d.description,'') description \n" +
            "   , NULLIF(t.sbu,'') sbu \n" +
            "   , COALESCE(NULLIF(t.pc,''), NULLIF(t.pc2,'')) profitcentre \n" +
            "   , NULLIF(d.review,'') review \n" +
            "   , CAST(NULL as varchar) journalElsComInd \n" +
            "   , CAST(NULL as varchar) journalAimsScope \n" +
            "   , CAST(NULL as varchar) ddpEligibInd\n" +
            "   , CAST(NULL as varchar) ptsJournalInd\n" +
            "   , CAST(NULL as varchar) authorFeedbackInd\n" +
            "   , CAST(NULL as varchar) deltaBusinessUnit\n" +
            "   , CAST(NULL as varchar) printerName\n" +
            "   , CAST(NULL as varchar) primarySiteSystem\n" +
            "   , CAST(NULL as varchar) primarySiteAcronym\n" +
            "   , CAST(NULL as varchar) primarySiteSupportLevel\n" +
            "   , CAST(NULL as varchar) fulfilmentSystem\n" +
            "   , CAST(NULL as varchar) fulfilmentJournalAcronym\n" +
            "   , CAST(NULL as varchar) issueProdTypeCode\n" +
            "   , CAST(NULL as integer) catalogueVolumesQty\n" +
            "   , CAST(NULL as integer) catalogueIssuesQty\n" +
            "   , CAST(NULL as varchar) catalogueVolumeFrom\n" +
            "   , CAST(NULL as varchar) catalogueVolumeTo\n" +
            "   , CAST(NULL as integer) rfIssuesQty\n" +
            "   , CAST(NULL as integer) rfTotalPagesQty\n" +
            "   , CAST(NULL as varchar) rfFvi\n" +
            "   , CAST(NULL as varchar) rfLvi\n" +
            "   , CAST(NULL as varchar) journalPreviousTitle\n" +
            "   , CAST(NULL as varchar) journalPrimaryAuthor\n" +
            "   FROM\n" +
            "     " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content c\n" +
            "   INNER JOIN " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((c.sourceref = v.sourceref) AND (v.workmasterprojectno = v.sourceref))\n" +
            "   LEFT JOIN (select sourceref \n" +
            "   , max(case when split_part(classificationcode, ' | ',1) = 'PTTR' then split_part(value, ' | ', 1) end) as textreftrade\n" +
            "   , max(case when substr(classificationcode,1,3) like 'SBU%' then substr(classificationcode, 4, 3) end) as sbu\n" +
            "   , max(case when substr(classificationcode,1,3) like 'SBU%' then substr(classificationcode, (strpos(classificationcode, 'PC') + 2), 2) end) as pc\n" +
            "   , max(case when substr(classificationcode,1,2) like 'PC%'  then replace(split_part(classificationcode, ' | ', 1), 'PC') end) as pc2\n" +
            "   from " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification\n" +
            "   where (case when split_part(classificationcode, ' | ',1) in ('PTTR','DCDFAC') then true\n" +
            "           when substr(classificationcode,1,3) like 'SBU%' then true\n" +
            "           when substr(classificationcode,1,2) like 'PC%' then true\n" +
            "           else false end) = true\n" +
            "   group by sourceref) t ON ((t.sourceref = c.sourceref))\n" +
            "   LEFT JOIN (select sourceref, max(case when substr(name,1,strpos(name,'|')-2)='BEBULLCPY' then text end) as bebullcpy\n" +
            "   , max(case when substr(name,1,strpos(name,'|')-2)='SEMSBKA' then text end) as semsbka\n" +
            "   , max(case when substr(name,1,strpos(name,'|')-2)='BETOCLNG' then text end) as betoclng\n" +
            "   , max(case when substr(name,1,strpos(name,'|')-2)='BETOCSHT' then text end) as betocsht\n" +
            "   , max(case when substr(name,1,strpos(name,'|')-2)='SEMKTAUD' then text end) as semktaud\n" +
            "   , max(case when substr(name,1,strpos(name,'|')-2)='BEAUABYL' then text end) as beaubyl\n" +
            "   from " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_text\n" +
            "   where substr(name,1,strpos(name,'|')-2) in ('BEBULLCPY','SEMSBKA','BETOCLNG','BETOCSHT','SEMKTAUD','BEAUABYL')\n" +
            "   group by sourceref\n" +
            "   )  f ON (c.sourceref = f.sourceref)\n" +
            "   LEFT JOIN (SELECT text.sourceref \n" +
            "       , max(case when texttype.purpose = 'Review' then text.text end) as review\n" +
            "       , max(case when texttype.purpose = 'Description' then text.text end) as description\n" +
            "      FROM\n" +
            "        " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_text text\n" +
            "      INNER JOIN (\n" +
            "         SELECT\n" +
            "           content.sourceref \n" +
            "         , texttypecode.texttype \n" +
            "         , texttypecode.purpose \n" +
            "         FROM\n" +
            "           " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
            "         INNER JOIN " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".texttypecode ON ((content.objecttype = texttypecode.objecttype) AND (content.companygroup = texttypecode.companygroup))\n" +
            "         WHERE (texttypecode.purpose in ('Review','Description'))\n" +
            "      )  texttype ON ((text.sourceref = texttype.sourceref) AND (split_part(text.name, ' | ', 1) = texttype.texttype))\n" +
            "      group by text.sourceref \n" +
            "   )  d ON (c.sourceref = d.sourceref)\n" +
            ")A\n" +
            "INNER JOIN " +GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
            "A.sourceref = cr.identifier AND \n" +
            "cr.identifier_type = 'external_reference' AND \n" +
            "cr.record_level = 'Work'\n" +
            "WHERE A.metadeleted = FALSE \n" +
            ")";

    public static String GET_WORK_SUBJECT_AREA_INBOUND_CURRENT_COUNT=
            "select count(*) as Source_Count from (\n" +
            "SELECT distinct cr.epr eprId, concat(A.sourceref,A.typecode,A.subjcode,A.subjdesc,coalesce(A.priority,'')) u_key, cr.work_type, A.* FROM ( \n" +
            "SELECT DISTINCT\n" +
            "     NULLIF(w.sourceref,'') sourceref \n" +
            "   , date_parse(NULLIF(w.metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon \n" +
            "   , 'PROMIS' typecode \n" +
            "   , 'Promis' typedesc \n" +
            "   , substr(split_part(NULLIF(c.classificationcode,''), ' | ', 1), (length(split_part(NULLIF(c.classificationcode,''), ' | ', 1)) - 4)) subjcode \n" +
            "   , split_part (NULLIF(c.classificationcode,''), ' | ', 2) subjdesc \n" +
            "   , NULLIF(c.priority,'') priority \n" +
            "   , CASE WHEN w.metadeleted = 'Y' THEN true ELSE false END metadeleted \n" +
            "   FROM\n" +
            "     ((" +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w\n" +
            "   INNER JOIN " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((w.sourceref = v.sourceref) AND (v.sourceref = v.workmasterprojectno)))\n" +
            "   INNER JOIN " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((w.sourceref = c.sourceref) AND (c.classificationcode LIKE 'PROMIS%')))\n" +
            "UNION    SELECT DISTINCT\n" +
            "     NULLIF(sourceref,'') sourceref \n" +
            "   , date_parse(NULLIF(modifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon \n" +
            "   , NULLIF(typecode,'') typecode \n" +
            "   , NULLIF(typedesc,'') typedesc \n" +
            "   , NULLIF(subjcode,'') subjcode \n" +
            "   , NULLIF(subjdesc,'') subjdesc \n" +
            "   , NULLIF(priority,'') priority \n" +
            "   , \"metadeleted\"\n" +
            "   FROM\n" +
            "     (\n" +
            "      SELECT DISTINCT\n" +
            "        w.sourceref\n" +
            "      , \"w\".\"metamodifiedon\" \"modifiedon\"\n" +
            "      , 'MSC' typecode \n" +
            "      , 'Elsevier HS Major Subject Codes' typedesc \n" +
            "      , replace(split_part(c.classificationcode, ' | ', 1), 'MSC') subjcode \n" +
            "      , split_part(c.classificationcode, ' | ', 2) subjdesc \n" +
            "      , c.priority \n" +
            "      , CASE WHEN w.metadeleted = 'Y' THEN true ELSE false END metadeleted \n" +
            "      FROM\n" +
            "        ((" +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w\n" +
            "      INNER JOIN " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((w.sourceref = v.sourceref) AND (v.sourceref = v.workmasterprojectno)))\n" +
            "      INNER JOIN " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((w.sourceref = c.sourceref) AND (c.classificationcode LIKE 'MSC%')))\n" +
            "UNION       SELECT DISTINCT\n" +
            "        w.sourceref \n" +
            "      , w.metamodifiedon modifiedon \n" +
            "      , 'MSC' typecode \n" +
            "      , 'Elsevier HS Major Subject Codes' typedesc \n" +
            "      , substr(split_part(c.classificationcode, ' | ', 1), (strpos(split_part(c.classificationcode, ' | ', 1), 'MSC') + 3), 3) subjcode \n" +
            "      , split_part (c.classificationcode, ' | ', 2) subjdesc \n" +
            "      , c.priority \n" +
            "      , CASE WHEN w.metadeleted = 'Y' THEN true ELSE false END metadeleted\n" +
            "      FROM\n" +
            "        ((" +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w\n" +
            "      INNER JOIN " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((w.sourceref = v.sourceref) AND (v.sourceref = v.workmasterprojectno)))\n" +
            "      INNER JOIN " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((w.sourceref = c.sourceref) AND (c.classificationcode LIKE 'SBU%MSC%')))\n" +
            "   )\n" +
            "UNION    SELECT DISTINCT\n" +
            "     NULLIF(w.sourceref,'') sourceref \n" +
            "   , date_parse(NULLIF(w.metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon\n" +
            "   , 'BRASESUBDI' typecode\n" +
            "   , 'Brazil Segmento, Subposicionamento & Disciplina' typedesc \n" +
            "   , replace(split_part(NULLIF(c.classificationcode,''), ' | ', 1), 'BRASESUBDI') subjcode \n" +
            "   , split_part(NULLIF(c.classificationcode,''), ' | ', 2) subjdesc \n" +
            "   , NULLIF(c.priority,'') priority \n" +
            "   , CASE WHEN w.metadeleted = 'Y' THEN true ELSE false END metadeleted \n" +
            "   FROM\n" +
            "     ((" +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w\n" +
            "   INNER JOIN " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((w.sourceref = v.sourceref) AND (v.sourceref = v.workmasterprojectno)))\n" +
            "   INNER JOIN " +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((w.sourceref = c.sourceref) AND (c.classificationcode LIKE 'BRASESUBDI%')))\n" +
            ")A\n" +
            "INNER JOIN " +GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON \n" +
            "A.sourceref = cr.identifier AND \n" +
            "cr.identifier_type = 'external_reference' AND \n" +
            "cr.record_level = 'Work'\n" +
            "WHERE A.metadeleted = FALSE \n" +
            ")";


    public static String GET_MANIFESTATION_RESTRICTIONS_INBOUND_CURRENT_COUNT=
            "select count(*) as Source_Count from (\n" +
                    "SELECT distinct cr.epr eprId, concat(A.sourceref, A.restrictioncode) u_key, cr.manifestation_type, A.* FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(c.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(c.metamodifiedon,''),'%d-%b-%Y %H:%i:%s') modifiedon\n" +
                    "   , split_part(NULLIF(c.value,''), ' | ', 1) restrictioncode\n" +
                    "   , split_part(NULLIF(c.value,''), ' | ', 2) restrictionname\n" +
                    "   , CASE WHEN c.metadeleted = 'Y' THEN true ELSE false END metadeleted\n" +
                    "   FROM\n" +
                    "     (" +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c)\n" +
                    "   WHERE (classificationcode LIKE 'MARESTR%') AND (trim(value) != '')\n" +
                    ")A\n" +
                    "INNER JOIN " +GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Manifestation'\n" +
                    "WHERE A.metadeleted = FALSE)";


    public static String GET_PRODUCT_PRICES_INBOUND_CURRENT_COUNT=
            "select count(*) as Source_Count from (\n" +
                    "SELECT DISTINCT\n" +
                    "     cr.epr eprId\n" +
                    "   , cr.product_type\n" +
                    "   , concat(coalesce(A.sourceref, ''), coalesce(split_part(NULLIF(type, ''), ' | ', 1),''), coalesce(currency, ''), coalesce(validfrom, ''), coalesce(validto, ''), coalesce(cast(price as varchar), '')) u_key \n" +
                    "   , A.sourceref sourceref\n" +
                    "   , date_parse(NULLIF(metamodifiedon, ''), '%d-%b-%Y %H:%i:%s') modifiedon\n" +
                    "   , NULLIF(currency, '') priceCurrency\n" +
                    "   , price priceAmount\n" +
                    "   , CAST(date_parse(NULLIF(validfrom, ''), '%d-%b-%Y') AS date) priceStartDate\n" +
                    "   , CAST(date_parse(NULLIF(validto, ''), '%d-%b-%Y') AS date) priceEndDate\n" +
                    "   , CAST(null AS varchar) priceRegion\n" +
                    "   , split_part(NULLIF(type, ''),' | ',1) priceCategory\n" +
                    "   , CAST(null AS varchar) priceCustomerCategory\n" +
                    "   , CAST(null AS Integer) pricePurchaseQuantity\n" +
                    "   , (CASE WHEN (metadeleted = 'Y') THEN true ELSE false END) metadeleted\n" +
                    "   FROM\n" +
                    "     (" +GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_pricing A\n" +
                    "   INNER JOIN " +GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr\n" +
                    "ON (((A.sourceref = cr.identifier) \n" +
                    "AND (cr.identifier_type = 'external_reference'))\n" +
                    "AND (cr.record_level = 'Product'))))";

    public static String GET_WORK_PERSON_ROLE_INBOUND_CURRENT_COUNT="";

    public static String GET_AVAILABILITY_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_availability";

    public static String GET_MANIF_EXT_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation";

    public static String GET_PAGE_COUNT_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_page_count";

    public static String GET_URL_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_url";

    public static String GET_WORK_EXT_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work";

    public static String GET_WORK_SUBJ_AREA_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_subject_area";

    public static String GET_MANIF_REST_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation_restrictions";

    public static String GET_PROD_PRICE_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_product_prices";

    public static String GET_WORK_PERS_ROLE_DELTA_CURR_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_person_role";

    public static String GET_AVAILABILITY_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_availability_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_availability_part)";

    public static String GET_MANIF_EXT_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_manifestation_part " +
                    "where delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_manifestation_part)";

    public static String GET_PAGE_COUNT_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_page_count_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_page_count_part) ";

    public static String GET_URL_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_url_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_url_part) ";

    public static String GET_WORK_EXT_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_part) ";

    public static String GET_WORK_SUBJ_AREA_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_subject_area_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_subject_area_part) ";

    public static String GET_MANIF_REST_DELTA_HIST_CURR_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_manifestation_restrictions_part where" +
                    " delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_manifestation_restrictions_part) ";

    public static String GET_PROD_PRICE_DELTA_HIST_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_product_prices_part where" +
                    " delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_product_prices_part) ";

    public static String GET_WORK_PERS_ROLE_DELTA_HIST_COUNT =
            "select count(*) as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_person_role_part " +
                    "where delta_ts = (select max(delta_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_person_role_part)";

    public static String GET_AVAILABILITY_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_part)";

    public static String GET_MANIF_EXT_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_part)";

    public static String GET_PAGE_COUNT_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_part)";

    public static String GET_URL_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_part)";

    public static String GET_WORK_EXT_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_part)";

    public static String GET_WORK_SUBJ_AREA_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_part)";

    public static String GET_MANIF_REST_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_part)";

    public static String GET_PROD_PRICE_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_part)";

    public static String GET_WORK_PERS_ROLE_CURR_HIST_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_part)";


}



