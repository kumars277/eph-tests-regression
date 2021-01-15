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
            "WITH\n" +
                    "  isbn_derivation AS (\n" +
                    "   SELECT \n" +
                    "     sourceref \n" +
                    "   , isbn13 identifier \n" +
                    "   , 'ISBN' identifier_type \n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product\n" +
                    "   WHERE (isbn13 <> '') AND metadeleted = 'N')\n" +
            "select count(*) as Source_Count from (\n" +
                    "SELECT DISTINCT\n" +
                    "     cr.epr eprId\n" +
                    "   , cr.product_type\n" +
                    "   , concat(coalesce(A.sourceref, ''), coalesce(split_part(NULLIF(type, ''), ' | ', 1),''), coalesce(currency, ''), coalesce(validfrom, ''), coalesce(validto, ''), coalesce(cast(price as varchar), '')) u_key \n" +
                    "   , A.sourceref sourceref\n" +
                    "   , date_parse(NULLIF(metamodifiedon, ''), '%d-%b-%Y %H:%i:%s') modifiedon\n" +
                    "   , NULLIF(currency, '') priceCurrency \n" +
                    "   , price priceAmount \n" +
                    "   , CAST(date_parse(NULLIF(validfrom, ''), '%d-%b-%Y') AS date) priceStartDate\n" +
                    "   , CAST(date_parse(NULLIF(validto, ''), '%d-%b-%Y') AS date) priceEndDate\n" +
                    "   , CAST(null AS varchar) priceRegion \n" +
                    "   , split_part(NULLIF(type, ''),' | ',1) priceCategory\n" +
                    "   , CAST(null AS varchar) priceCustomerCategory\n" +
                    "   , CAST(null AS Integer) pricePurchaseQuantity\n" +
                    "   , (CASE WHEN (metadeleted = 'Y') THEN true ELSE false END) metadeleted\n" +
                    "   FROM\n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_pricing A\n" +
                    "   INNER JOIN " +GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr\n" +
                    "ON (((A.sourceref = cr.identifier) \n" +
                    "AND (cr.identifier_type = 'external_reference'))\n" +
                    "AND (cr.record_level = 'Product')))" +
                    "WHERE A.metadeleted = 'N')";

    public static String GET_WORK_PERSON_ROLE_INBOUND_CURRENT_COUNT=
            "WITH\n" +
                    "  isbn_derivation AS (\n" +
                    "   SELECT\n" +
                    "     sourceref\n" +
                    "   , isbn13 identifier\n" +
                    "   , 'ISBN' identifier_type \n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product\n" +
                    "   WHERE (isbn13 <> '')\n" +
                    ") \n" +
                    ", orignotes AS (\n" +
                    "   SELECT DISTINCT\n" +
                    "     businesspartnerid\n" +
                    "   , notestype\n" +
                    "   , notes \n" +
                    "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originatornotes\n" +
                    ") \n" +
                    "select count(*) as Source_Count from (\n" +
                    "SELECT\n" +
                    "  concat(concat(r.sourceref, 'MARMAN'), r.personid) u_key\n" +
                    ", NULLIF(id.identifier, '') worksourceref\n" +
                    ", NULLIF(r.personid, '') personsourceref\n" +
                    ", 'BCS' source\n" +
                    ", cr.epr eprId\n" +
                    ", cr.work_type work_type\n" +
                    ", CAST(null AS integer) core_reference\n" +
                    ", 'MARMAN' roletype\n" +
                    ", 'Marketing Manager' rolename\n" +
                    ", CAST(null AS varchar) title\n" +
                    ", split_part(split_part(r.responsibleperson, ',', 2), ' (', 1) person_first_name\n" +
                    ", split_part(r.responsibleperson, ',', 1) person_family_name\n" +
                    ", CAST(null AS varchar) email_address\n" +
                    ", CAST(null AS varchar) honours\n" +
                    ", CAST(null AS varchar) affiliation\n" +
                    ", CAST(null AS varchar) imageUrl\n" +
                    ", CAST(null AS varchar) footnoteTxt\n" +
                    ", CAST(null AS varchar) notesTxt\n" +
                    ", (CASE WHEN (substr(split_part(NULLIF(r.responsibility, ''), ' | ', 1), -1, 1) = '2') THEN '2' ELSE '1' END) sequence\n" +
                    ", CAST(null AS integer) groupNumber\n" +
                    ", date_parse(NULLIF(r.metamodifiedon, ''), '%d-%b-%Y %H:%i:%s') metamodifiedon\n" +
                    ", (CASE WHEN (r.metadeleted = 'Y') THEN true ELSE false END) metadeleted\n" +
                    "FROM\n" +
                    "  (("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_responsibilities r\n" +
                    "INNER JOIN isbn_derivation id ON ((r.sourceref = id.sourceref) AND (id.identifier_type = 'ISBN')))\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON (((id.identifier = cr.identifier) AND (cr.identifier_type = 'ISBN')) AND (cr.record_level = 'Work')))\n" +
                    "UNION ALL SELECT\n" +
                    "  concat(concat(o.sourceref, 'MARMAN'), CAST(o.businesspartnerid AS varchar)) u_key\n" +
                    ", NULLIF(id.identifier, '') worksourceref\n" +
                    ", NULLIF(CAST(o.businesspartnerid AS varchar), '') personsourceref\n" +
                    ", 'BCS' source\n" +
                    ", cr.epr eprId\n" +
                    ", cr.work_type work_type\n" +
                    ", gwp.work_person_role_id core_reference\n" +
                    ", NULLIF(rolecode.ephcode, '') roletype\n" +
                    ", NULLIF(rolecode.ephname, '') rolename\n" +
                    ", CAST(null AS varchar) title\n" +
                    ", (CASE WHEN (isperson = 'N') THEN NULLIF(department, '') ELSE NULLIF(firstname, '') END) person_first_name\n" +
                    ", (CASE WHEN (isperson = 'N') THEN NULLIF(institution, '') ELSE NULLIF(lastname, '') END) person_family_name\n" +
                    ", CAST(null AS varchar) email_address\n" +
                    ", h.notes honours\n" +
                    ", a.notes affiliation\n" +
                    ", concat(concat('https://covers.elsevier.com/author/186/1', lpad(CAST(o.businesspartnerid AS varchar), 6, '000000')), '.jpg') imageUrl\n" +
                    ", CAST(null AS varchar) footnoteTxt\n" +
                    ", n.notes notesTxt\n" +
                    ", CAST(o.sequence AS varchar)\n" +
                    ", CAST(null AS integer) groupNumber\n" +
                    ", date_parse(NULLIF(o.metamodifiedon, ''), '%d-%b-%Y %H:%i:%s') modifiedon\n" +
                    ", (CASE WHEN (o.metadeleted = 'Y') THEN true ELSE false END) metadeleted\n" +
                    "FROM\n" +
                    "  ((((((("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originators o\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(copyrightholdertype, ' | ', 1) = rolecode.ppmcode))\n" +
                    "INNER JOIN isbn_derivation id ON ((o.sourceref = id.sourceref) AND (id.identifier_type = 'ISBN')))\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON (((id.identifier = cr.identifier) AND (cr.identifier_type = 'ISBN')) AND (cr.record_level = 'Work')))\n" +
                    "LEFT JOIN orignotes h ON ((o.businesspartnerid = h.businesspartnerid) AND (split_part(h.notestype, ' | ', 1) = 'DEG')))\n" +
                    "LEFT JOIN orignotes a ON ((o.businesspartnerid = a.businesspartnerid) AND (split_part(a.notestype, ' | ', 1) = 'AFIL')))\n" +
                    "LEFT JOIN orignotes n ON ((o.businesspartnerid = n.businesspartnerid) AND (split_part(n.notestype, ' | ', 1) = 'BIO')))\n" +
                    "INNER JOIN "+GetBCS_ETLCoreDLDBUser.getProdDataBase()+".gd_work_person_role gwp ON ((concat(o.sourceref, rolecode.ephcode, CAST(o.businesspartnerid AS varchar)) = gwp.external_reference) AND (gwp.effective_end_date IS NULL))))";
                    //"WHERE o.metadeleted = 'N'\n";

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

    public static String GET_AVAILABILITY_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part)";

    public static String GET_MANIF_EXT_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part)";

    public static String GET_PAGE_COUNT_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part)";

    public static String GET_URL_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part)";

    public static String GET_WORK_EXT_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part)";

    public static String GET_WORK_SUBJ_AREA_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part)";

    public static String GET_MANIF_REST_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part)";

    public static String GET_PROD_PRICE_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part)";

    public static String GET_WORK_PERS_ROLE_CURR_FILE_COUNT =
            "select count(*) as Source_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part)";



    public static String GET_AVAILABILTIY_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, application, deltaanswercodeuk, deltaanswercodeus, anzpubstatus,pubdateactual,status,metadeleted,producttype\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, application, deltaanswercodeuk, deltaanswercodeus, anzpubstatus,pubdateactual,status,metadeleted,producttype\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as target_Count from( \n" +
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
                    "    where (coalesce(crr.eprid,'na') <> coalesce(prev.eprid,'na') or\n" +
                    "            coalesce(crr.u_key,'na') <> coalesce(prev.u_key,'na') or\n" +
                    "            coalesce (crr.sourceref, 'na') <> coalesce (prev.sourceref, 'na') or\n" +
                    "            coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    "            coalesce (crr.application, 'na') <> coalesce (prev.application, 'na') or\n" +
                    "            coalesce (crr.deltaanswercodeuk, 'na') <> coalesce (prev.deltaanswercodeuk, 'na') or\n" +
                    "            coalesce (crr.deltaanswercodeus, 'na') <> coalesce (prev.deltaanswercodeus, 'na') or \n" +
                    "            coalesce (crr.anzpubstatus, 'na') <> coalesce (prev.anzpubstatus, 'na') or \n" +
                    "            coalesce (crr.pubdateactual, current_date) <> coalesce (prev.pubdateactual, current_date) or \n" +
                    "            coalesce (crr.status, 'na') <> coalesce (prev.status, 'na') or \n" +
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false) or \n" +
                    "            coalesce (crr.producttype, 'na') <> coalesce (prev.producttype, 'na')))";

    public static String GET_MANIF_EXT_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, uktextbookind, ustextbookind,usdiscountcode,usdiscountname,emeadiscountcode,emeadiscountname,trimsize,weight,commcode,journalprodsitecode,journalissuetrimsize,warreference\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, uktextbookind, ustextbookind,usdiscountcode,usdiscountname,emeadiscountcode,emeadiscountname,trimsize,weight,commcode,journalprodsitecode,journalissuetrimsize,warreference\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as target_Count from( \n" +
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
                    "    where (coalesce(crr.eprid,'na') <> coalesce(prev.eprid,'na') or\n" +
                    "            coalesce(crr.u_key,'na') <> coalesce(prev.u_key,'na') or\n" +
                    "            coalesce (crr.sourceref, 'na') <> coalesce (prev.sourceref, 'na') or\n" +
                    "            coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false) or \n" +
                    "            coalesce (crr.manifestation_type, 'na') <> coalesce (prev.manifestation_type, 'na') or\n" +
                    "            coalesce (crr.uktextbookind, false) <> coalesce (prev.uktextbookind, false) or\n" +
                    "            coalesce (crr.ustextbookind, false) <> coalesce (prev.ustextbookind, false) or \n" +
                    "            coalesce (crr.usdiscountcode, 'na') <> coalesce (prev.usdiscountcode, 'na') or \n" +
                    "            coalesce (crr.usdiscountname, 'na') <> coalesce (prev.usdiscountname, 'na') or \n" +
                    "            coalesce (crr.emeadiscountcode, 'na') <> coalesce (prev.emeadiscountcode, 'na') or \n" +
                    "            coalesce (crr.emeadiscountname, 'na') <> coalesce (prev.emeadiscountname, 'na') or \n" +
                    "            coalesce (crr.trimsize, 'na') <> coalesce (prev.trimsize, 'na') or \n" +
                    "            coalesce (crr.weight, 0) <> coalesce (prev.weight, 0) or \n" +
                    "            coalesce (crr.commcode, 'na') <> coalesce (prev.commcode, 'na') or \n" +
                    "            coalesce (crr.journalprodsitecode, 'na') <> coalesce (prev.journalprodsitecode, 'na') or \n" +
                    "            coalesce (crr.journalissuetrimsize, 'na') <> coalesce (prev.journalissuetrimsize, 'na') or \n" +
                    "            coalesce (crr.warreference, 'na') <> coalesce (prev.warreference, 'na')))";

    public static String GET_PAGE_COUNT_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, pagecounttypecode, pagecounttypename,pagecount\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, pagecounttypecode, pagecounttypename,pagecount\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as target_Count from( \n" +
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
                    "    where (coalesce(crr.eprid,'na') <> coalesce(prev.eprid,'na') or\n" +
                    "            coalesce(crr.u_key,'na') <> coalesce(prev.u_key,'na') or\n" +
                    "            coalesce (crr.sourceref, 'na') <> coalesce (prev.sourceref, 'na') or\n" +
                    "            coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false) or \n" +
                    "            coalesce (crr.manifestation_type, 'na') <> coalesce (prev.manifestation_type, 'na') or\n" +
                    "            coalesce (crr.pagecounttypecode, 'na') <> coalesce (prev.pagecounttypecode, 'na') or \n" +
                    "            coalesce (crr.pagecounttypename, 'na') <> coalesce (prev.pagecounttypename, 'na') or \n" +
                    "            coalesce (crr.pagecount, 0) <> coalesce (prev.pagecount, 0)))";

    public static String GET_URL_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, urltypecode, urltypename, source, name\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, urltypecode, urltypename, source, name\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part dhap)where rn = 2))                                  \n" +
                    "select count(*) as target_Count from( \n" +
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
                    "    where (coalesce(crr.eprid,'na') <> coalesce(prev.eprid,'na') or\n" +
                    "            coalesce(crr.u_key,'na') <> coalesce(prev.u_key,'na') or\n" +
                    "            coalesce (crr.sourceref, 'na') <> coalesce (prev.sourceref, 'na') or\n" +
                    "            coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false) or \n" +
                    "            coalesce (crr.work_type, 'na') <> coalesce (prev.work_type, 'na') or\n" +
                    "            coalesce (crr.urltypecode, 'na') <> coalesce (prev.urltypecode, 'na') or \n" +
                    "            coalesce (crr.urltypename, 'na') <> coalesce (prev.urltypename , 'na') or \n" +
                    "            coalesce (crr.source, 'na') <> coalesce (prev.source, 'na') or \n" +
                    "            coalesce (crr.name, 'na') <> coalesce (prev.name, 'na')))";

    public static String GET_WRK_EXT_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, companygroup, imagefileref, workmasterisbn, textreftrade, features,awards,toc_long,toc_short,audience,authorbyline,description,sbu,profitcentre,review,journalelscomind,journalaimsscope,ddpeligibind,ptsjournalind,authorfeedbackind,deltabusinessunit,printername,primarysitesystem,primarysiteacronym,primarysitesupportlevel,fulfilmentsystem,fulfilmentjournalacronym,issueprodtypecode,cataloguevolumesqty,catalogueissuesqty,cataloguevolumefrom,cataloguevolumeto,rfissuesqty,rftotalpagesqty,rffvi,rflvi,journalprevioustitle,journalprimaryauthor\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, companygroup, imagefileref, workmasterisbn, textreftrade, features,awards,toc_long,toc_short,audience,authorbyline,description,sbu,profitcentre,review,journalelscomind,journalaimsscope,ddpeligibind,ptsjournalind,authorfeedbackind,deltabusinessunit,printername,primarysitesystem,primarysiteacronym,primarysitesupportlevel,fulfilmentsystem,fulfilmentjournalacronym,issueprodtypecode,cataloguevolumesqty,catalogueissuesqty,cataloguevolumefrom,cataloguevolumeto,rfissuesqty,rftotalpagesqty,rffvi,rflvi,journalprevioustitle,journalprimaryauthor\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    "select count(*) as target_Count from( \n" +
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
                    "    where (coalesce(crr.eprid,'na') <> coalesce(prev.eprid,'na') or\n" +
                    "            coalesce(crr.u_key,'na') <> coalesce(prev.u_key,'na') or\n" +
                    "            coalesce (crr.sourceref, 'na') <> coalesce (prev.sourceref, 'na') or\n" +
                    "            coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false) or \n" +
                    "            coalesce (crr.work_type, 'na') <> coalesce (prev.work_type, 'na') or\n" +
                    "            coalesce (crr.companygroup, 'na') <> coalesce (prev.companygroup, 'na') or \n" +
                    "            coalesce (crr.imagefileref, 'na') <> coalesce (prev.imagefileref , 'na') or \n" +
                    "            coalesce (crr.workmasterisbn, 'na') <> coalesce (prev.workmasterisbn, 'na') or \n" +
                    "            coalesce (crr.textreftrade, 'na') <> coalesce (prev.textreftrade, 'na') or \n" +
                    "            coalesce (crr.features, 'na') <> coalesce (prev.features, 'na') or \n" +
                    "            coalesce (crr.awards, 'na') <> coalesce (prev.awards, 'na') or \n" +
                    "            coalesce (crr.toc_long, 'na') <> coalesce (prev.toc_long, 'na') or \n" +
                    "            coalesce (crr.toc_short, 'na') <> coalesce (prev.toc_short, 'na') or \n" +
                    "            coalesce (crr.audience, 'na') <> coalesce (prev.audience, 'na') or \n" +
                    "            coalesce (crr.authorbyline, 'na') <> coalesce (prev.authorbyline, 'na') or \n" +
                    "            coalesce (crr.description, 'na') <> coalesce (prev.description, 'na') or \n" +
                    "            coalesce (crr.sbu, 'na') <> coalesce (prev.sbu, 'na') or \n" +
                    "            coalesce (crr.profitcentre, 'na') <> coalesce (prev.profitcentre, 'na') or \n" +
                    "            coalesce (crr.review, 'na') <> coalesce (prev.review, 'na') or \n" +
                    "            coalesce (crr.journalelscomind, 'na') <> coalesce (prev.journalelscomind, 'na') or \n" +
                    "            coalesce (crr.journalaimsscope, 'na') <> coalesce (prev.journalaimsscope, 'na') or \n" +
                    "            coalesce (crr.ddpeligibind, 'na') <> coalesce (prev.ddpeligibind, 'na') or \n" +
                    "            coalesce (crr.ptsjournalind, 'na') <> coalesce (prev.ptsjournalind, 'na') or \n" +
                    "            coalesce (crr.authorfeedbackind, 'na') <> coalesce (prev.authorfeedbackind, 'na') or \n" +
                    "            coalesce (crr.deltabusinessunit, 'na') <> coalesce (prev.deltabusinessunit, 'na') or \n" +
                    "            coalesce (crr.printername, 'na') <> coalesce (prev.printername, 'na') or \n" +
                    "            coalesce (crr.primarysitesystem, 'na') <> coalesce (prev.primarysitesystem, 'na') or \n" +
                    "            coalesce (crr.primarysiteacronym, 'na') <> coalesce (prev.primarysiteacronym, 'na') or \n" +
                    "            coalesce (crr.primarysitesupportlevel, 'na') <> coalesce (prev.primarysitesupportlevel, 'na') or \n" +
                    "            coalesce (crr.fulfilmentsystem, 'na') <> coalesce (prev.fulfilmentsystem, 'na') or \n" +
                    "            coalesce (crr.fulfilmentjournalacronym, 'na') <> coalesce (prev.fulfilmentjournalacronym, 'na') or \n" +
                    "            coalesce (crr.issueprodtypecode, 'na') <> coalesce (prev.issueprodtypecode, 'na') or \n" +
                    "            coalesce (crr.cataloguevolumesqty, 0) <> coalesce (prev.cataloguevolumesqty, 0) or \n" +
                    "            coalesce (crr.catalogueissuesqty, 0) <> coalesce (prev.catalogueissuesqty, 0) or \n" +
                    "            coalesce (crr.issueprodtypecode, 'na') <> coalesce (prev.issueprodtypecode, 'na') or \n" +
                    "            coalesce (crr.cataloguevolumesqty, 0) <> coalesce (prev.cataloguevolumesqty, 0) or \n" +
                    "            coalesce (crr.catalogueissuesqty, 0) <> coalesce (prev.catalogueissuesqty, 0) or \n" +
                    "            coalesce (crr.cataloguevolumefrom, 'na') <> coalesce (prev.cataloguevolumefrom, 'na') or \n" +
                    "            coalesce (crr.cataloguevolumeto, 'na') <> coalesce (prev.cataloguevolumeto, 'na') or \n" +
                    "            coalesce (crr.rfissuesqty, 0) <> coalesce (prev.rfissuesqty, 0) or \n" +
                    "            coalesce (crr.rftotalpagesqty, 0) <> coalesce (prev.rftotalpagesqty, 0) or \n" +
                    "            coalesce (crr.rffvi, 'na') <> coalesce (prev.rffvi, 'na') or \n" +
                    "            coalesce (crr.rflvi, 'na') <> coalesce (prev.rflvi, 'na') or \n" +
                    "            coalesce (crr.journalprevioustitle, 'na') <> coalesce (prev.journalprevioustitle, 'na') or \n" +
                    "            coalesce (crr.journalprimaryauthor, 'na') <> coalesce (prev.journalprimaryauthor, 'na')))";


    public static String GET_WRK_SUBJ_AREA_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, typecode, typedesc, subjcode, subjdesc, priority\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, typecode, typedesc, subjcode, subjdesc, priority\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    "select count(*) as target_Count from( \n" +
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
                    "    where (coalesce(crr.eprid,'na') <> coalesce(prev.eprid,'na') or\n" +
                    "            coalesce(crr.u_key,'na') <> coalesce(prev.u_key,'na') or\n" +
                    "            coalesce (crr.sourceref, 'na') <> coalesce (prev.sourceref, 'na') or\n" +
                    "            coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false) or \n" +
                    "            coalesce (crr.work_type, 'na') <> coalesce (prev.work_type, 'na') or\n" +
                    "            coalesce (crr.typecode, 'na') <> coalesce (prev.typecode, 'na') or \n" +
                    "            coalesce (crr.typedesc, 'na') <> coalesce (prev.typedesc , 'na') or \n" +
                    "            coalesce (crr.subjcode, 'na') <> coalesce (prev.subjcode, 'na') or \n" +
                    "            coalesce (crr.subjdesc, 'na') <> coalesce (prev.subjdesc, 'na') or \n" +
                    "            coalesce (crr.priority, 'na') <> coalesce (prev.priority, 'na')))";

    public static String GET_MANIF_RESTRICTION_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, restrictioncode, restrictionname\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, restrictioncode, restrictionname\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    "select count(*) as target_Count from( \n" +
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
                    "    where (coalesce(crr.eprid,'na') <> coalesce(prev.eprid,'na') or\n" +
                    "            coalesce(crr.u_key,'na') <> coalesce(prev.u_key,'na') or\n" +
                    "            coalesce (crr.manifestation_type, 'na') <> coalesce (prev.manifestation_type, 'na') or\n" +
                    "            coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false) or \n" +
                    "            coalesce (crr.sourceref, 'na') <> coalesce (prev.sourceref, 'na') or\n" +
                    "            coalesce (crr.restrictioncode, 'na') <> coalesce (prev.restrictioncode, 'na') or \n" +
                    "            coalesce (crr.restrictionname, 'na') <> coalesce (prev.restrictionname , 'na')))";


    public static String GET_PROD_PRICE_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, product_type, pricecurrency, priceamount, pricestartdate, priceenddate, priceregion,pricecategory,pricecustomercategory,pricepurchasequantity\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, product_type, pricecurrency, priceamount, pricestartdate, priceenddate, priceregion,pricecategory,pricecustomercategory,pricepurchasequantity\n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    "select count(*) as target_Count from( \n" +
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
                    "    where (coalesce(crr.eprid,'na') <> coalesce(prev.eprid,'na') or\n" +
                    "            coalesce(crr.u_key,'na') <> coalesce(prev.u_key,'na') or\n" +
                    "            coalesce (crr.sourceref, 'na') <> coalesce (prev.sourceref, 'na') or\n" +
                    "            coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false) or \n" +
                    "            coalesce (crr.product_type, 'na') <> coalesce (prev.product_type, 'na') or\n" +
                    "            coalesce (crr.pricecurrency, 'na') <> coalesce (prev.pricecurrency, 'na') or \n" +
                    "            coalesce (crr.priceamount, 0) <> coalesce (prev.priceamount , 0) or \n" +
                    "            coalesce (crr.pricestartdate, current_date) <> coalesce (prev.pricestartdate, current_date) or \n" +
                    "            coalesce (crr.priceenddate, current_date) <> coalesce (prev.priceenddate, current_date) or \n" +
                    "            coalesce (crr.priceregion, 'na') <> coalesce (prev.priceregion, 'na') or \n" +
                    "            coalesce (crr.pricecategory, 'na') <> coalesce (prev.pricecategory, 'na') or \n" +
                    "            coalesce (crr.pricecustomercategory, 'na') <> coalesce (prev.pricecustomercategory, 'na') or \n" +
                    "            coalesce (crr.pricepurchasequantity, 0) <> coalesce (prev.pricepurchasequantity, 0)))";

    public static String GET_WORK_PERSON_ROLE_DIFF_TRANSFORM_FILE_COUNT =
            " with crr_dataset as(\n" +
                    "  select u_key, worksourceref, personsourceref, source, eprid, work_type, core_reference, roletype,rolename,title,person_first_name,person_family_name,email_address,honours,affiliation,imageurl,footnotetxt,notestxt,sequence,groupnumber,modifiedon,metadeleted \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select u_key, worksourceref, personsourceref, source, eprid, work_type, core_reference, roletype,rolename,title,person_first_name,person_family_name,email_address,honours,affiliation,imageurl,footnotetxt,notestxt,sequence,groupnumber,modifiedon,metadeleted \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    "select count(*) as target_Count from( \n" +
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
                    "    where (coalesce(crr.u_key,'na') <> coalesce(prev.u_key,'na') or\n" +
                    "            coalesce(crr.worksourceref,'na') <> coalesce(prev.worksourceref,'na') or\n" +
                    "            coalesce (crr.personsourceref, 'na') <> coalesce (prev.personsourceref, 'na') or\n" +
                    "            coalesce (crr.source, 'na') <> coalesce (prev.source, 'na') or\n" +
                    "            coalesce (crr.eprid, 'na') <> coalesce (prev.eprid, 'na') or \n" +
                    "            coalesce (crr.work_type, 'na') <> coalesce (prev.work_type, 'na') or\n" +
                    "            coalesce (crr.core_reference, 0) <> coalesce (prev.core_reference, 0) or \n" +
                    "            coalesce (crr.roletype, 'na') <> coalesce (prev.roletype, 'na') or \n" +
                    "            coalesce (crr.rolename, 'na') <> coalesce (prev.rolename, 'na') or \n" +
                    "            coalesce (crr.title, 'na') <> coalesce (prev.title, 'na') or \n" +
                    "            coalesce (crr.person_first_name, 'na') <> coalesce (prev.person_first_name, 'na') or \n" +
                    "            coalesce (crr.person_family_name, 'na') <> coalesce (prev.person_family_name, 'na') or \n" +
                    "            coalesce (crr.email_address, 'na') <> coalesce (prev.email_address, 'na') or \n" +
                    "            coalesce (crr.honours, 'na') <> coalesce (prev.honours, 'na') or \n" +
                    "            coalesce (crr.affiliation, 'na') <> coalesce (prev.affiliation, 'na') or \n" +
                    "            coalesce (crr.imageurl, 'na') <> coalesce (prev.imageurl, 'na') or \n" +
                    "            coalesce (crr.footnotetxt, 'na') <> coalesce (prev.footnotetxt, 'na') or \n" +
                    "            coalesce (crr.notestxt, 'na') <> coalesce (prev.notestxt, 'na') or \n" +
                    "            coalesce (crr.sequence, 'na') <> coalesce (prev.sequence, 'na') or \n" +
                    "            coalesce (crr.groupnumber, 0) <> coalesce (prev.groupnumber, 0) or \n" +
                    "            coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or \n" +
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false)))";


    public static String GET_AVAILABILITY_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_availability d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_part c ))";

    public static String GET_MANIF_EXT_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_part c ))";

    public static String GET_PAGE_COUNT_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_page_count d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_part c ))";

    public static String GET_URL_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_url d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_part c ))";


    public static String GET_WORK_EXT_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_part c ))";

    public static String GET_WRK_SUBJ_AREA_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_subject_area d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_part c ))";

    public static String GET_MANIF_RESTRICT_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation_restrictions d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_part c ))";

    public static String GET_PROD_PRICE_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_product_prices d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_part c ))";

    public static String GET_WORK_PERS_ROLE_DIFF_DELTA_AND_HIST_COUNT =
            "select count(*) as Source_Count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_part c\n" +
                    "left join "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_person_role d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_part c ))";


    public static String GET_AVAILABILITY_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_excl_delta";

    public static String GET_MANIF_EXT_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_excl_delta";

    public static String GET_PAGE_COUNT_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_excl_delta";

    public static String GET_URL_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_excl_delta";

    public static String GET_WRK_EXT_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_excl_delta";

    public static String GET_WRK_SUBJ_AREA_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_excl_delta";

    public static String GET_MANIF_RESTRICT_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_excl_delta";

    public static String GET_PROD_PRICE_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_excl_delta";

    public static String GET_WORK_PERS_ROLE_EXCL_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_excl_delta";


    public static String GET_AVAILABILITY_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_excl_delta as c union all \n" +
                    "select d.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_availability as d)";

    public static String GET_MANIF_EXT_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_excl_delta as c union all \n" +
                    "select d.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation as d)";

    public static String GET_PAGE_COUNT_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_excl_delta as c union all \n" +
                    "select d.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_page_count as d)";

    public static String GET_URL_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_excl_delta as c union all \n" +
                    "select d.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_url as d)";

    public static String GET_WRK_EXT_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_excl_delta as c union all \n" +
                    "select d.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work as d)";

    public static String GET_WRK_SUBJ_AREA_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_excl_delta as c union all \n" +
                    "select d.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_subject_area as d)";

    public static String GET_MANIF_RESTRICT_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_excl_delta as c union all \n" +
                    "select d.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation_restrictions as d)";

    public static String GET_ROD_PRICE_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_excl_delta as c union all \n" +
                    "select d.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_product_prices as d)";

    public static String GET_WORK_PERS_ROLE_SUM_DELTACURR_EXCL_COUNT =
            "select count(*) as source_count from \n" +
                    "(select c.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_excl_delta as c union all \n" +
                    "select d.u_key from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_person_role as d)";

    public static String GET_AVAILABILITY_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_latest";

    public static String GET_MANIF_EXT_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_latest";

    public static String GET_PAGE_COUNT_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_latest";

    public static String GET_URL_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_latest";

    public static String GET_WRK_EXT_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_latest";

    public static String GET_WRK_SUBJ_AREA_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_latest";

    public static String GET_MANIF_RESTRICT_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_latest";

    public static String GET_WRK_PERSON_ROLE_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_latest";


    public static String GET_PROD_PRICE_LATEST_COUNT =
            "select count(*)as Target_Count from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_latest";

    public static String GET_DUPLICATES_LATEST_AVAILABILITY_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_latest " +
                    "group by u_key having count(*)>1)";

    public static String GET_DUPLICATES_LATEST_MANIF_EXT_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_latest " +
                    "group by u_key having count(*)>1)";

    public static String GET_DUPLICATES_LATEST_PAGE_COUNT_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_latest " +
                    "group by u_key having count(*)>1)";

    public static String GET_DUPLICATES_LATEST_URL_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_latest " +
                    "group by u_key having count(*)>1)";

    public static String GET_DUPLICATES_LATEST_WORK_EXT_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_latest " +
                    "group by u_key having count(*)>1)";

    public static String GET_DUPLICATES_LATEST_WORK_SUBJ_AREA_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_latest " +
                    "group by u_key having count(*)>1)";

    public static String GET_DUPLICATES_LATEST_MANIF_RESTRICT_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_latest " +
                    "group by u_key having count(*)>1)";

    public static String GET_DUPLICATES_LATEST_PROD_PRICE_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_latest " +
                    "group by u_key having count(*)>1)";

    public static String GET_DUPLICATES_LATEST_WORK_PERS_ROLE_COUNT =
            "select count(*) as Duplicate_Count " +
                    "from (SELECT count(*) FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_latest " +
                    "group by u_key having count(*)>1)";


}



