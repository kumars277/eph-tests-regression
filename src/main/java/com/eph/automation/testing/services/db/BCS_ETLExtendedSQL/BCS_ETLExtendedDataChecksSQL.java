package com.eph.automation.testing.services.db.BCS_ETLExtendedSQL;


import com.eph.automation.testing.services.db.BCS_ETLExtendedSQL.GetBCS_ETLExtendedDLDBUser;

public class BCS_ETLExtendedDataChecksSQL {

    public static String GET_RANDOM_AVAILABILITY_KEY_INBOUND =
            "select eprid as EPRID from (\n" +
                    "SELECT distinct cr.epr eprId, concat(A.sourceref,A.application) u_key, cr.product_type productType, A.* FROM ( \n" +
                    "SELECT \n" +
                    "     NULLIF(p.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(p.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , 'Delta Books' application \n" +
                    "   , split_part(NULLIF(duk.value,''), ' | ', 1) deltaanswercodeuk \n" +
                    "   , split_part(NULLIF(dus.value,''), ' | ', 1) deltaanswercodeus \n" +
                    "   , split_part (NULLIF(dan.value,''), ' | ', 1) anzpubstatus \n" +
                    "   , cast(null as date) pubdateactual \n" +
                    "   , cast(null as varchar) status \n" +
                    "   , CASE WHEN p.metadeleted = 'Y' THEN true else false END metadeleted \n" +
                    "   FROM\n" +
                    "     ((("+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product p \n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification duk ON ((p.sourceref = duk.sourceref) AND (split_part(duk.classificationcode, ' | ', 1) = 'DCADA')))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification dus ON ((p.sourceref = dus.sourceref) AND (split_part(dus.classificationcode, ' | ', 1) = 'DCAADAUS')))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification dan ON ((p.sourceref = dan.sourceref) AND (split_part(dan.classificationcode, ' | ', 1) = 'DCAANZ')))\n" +
                    "UNION    SELECT \n" +
                    "     NULLIF(sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , NULLIF(locationcode.ephcode,'') application \n" +
                    "   , null deltaanswercodeuk \n" +
                    "   , null deltaanswercodeus \n" +
                    "   , null anzpubstatus \n" +
                    "   , cast(date_parse(coalesce(NULLIF(pubdateactual,''),NULLIF(plannedpubdate,'')),'%%d-%%b-%%Y') as date) pubdateactual \n" +
                    "   , split_part (NULLIF(status,''), ' | ', 1) status \n" +
                    "   , CASE WHEN metadeleted = 'Y' THEN true else false END metadeleted \n" +
                    "   FROM\n" +
                    "     "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation \n" +
                    "   INNER JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".locationcode ON (split_part(sublocation.warehouse, ' | ', 1) = locationcode.ppmcode)\n" +
                    "   WHERE NULLIF(trim(sublocation.refkey,' '),'') IS NOT NULL\n" +
                    "UNION SELECT\n" +
                    "     NULLIF(product.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(product.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , NULLIF(locationcode.ephcode,'') application \n" +
                    "   , null deltaanswercodeuk \n" +
                    "   , null deltaanswercodeus \n" +
                    "   , null anzpubstatus \n" +
                    "   , cast(date_parse(coalesce(NULLIF(product.publishedon,''),NULLIF(product.pubdateplanned,'')),'%%d-%%b-%%Y') as date) pubdateactual \n" +
                    "   , split_part (NULLIF(product.deliverystatus,''), ' | ', 1) status \n" +
                    "   , CASE WHEN product.metadeleted = 'Y' THEN true else false END metadeleted \n" +
                    "   FROM \n" +
                    "     "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                    "   INNER JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content on product.sourceref = content.sourceref \n" +
                    "   INNER JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".locationcode ON (content.ownership = locationcode.ppmcode)\n" +
                    "   WHERE NULLIF(trim(product.refkey,' '),'') IS NOT NULL  \n" +
                    ")A \n" +
                    "INNER JOIN " + GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON \n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Product'\n" +
                    "WHERE A.metadeleted = FALSE ) order by rand() limit %s \n";

    public static String GET_AVAILABILITY_REC_INBOUND_DATA =
            "select u_key as UKEY " +
                    ",sourceref as SOURCEREF" +
                    ",eprid as EPRID" +
                    ",producttype as PRODUCTTYPE" +
                    ",modifiedon as MODIFIEDON" +
                    ",application as APPLICATION" +
                    ",deltaanswercodeuk as deltaanswercodeuk" +
                    ",deltaanswercodeus as deltaanswercodeus" +
                    ",anzpubstatus as anzpubstatus" +
                    ",pubdateactual as pubdateactual" +
                    ",status as status" +
                    ",metadeleted as metadeleted" +
                    " from (\n" +
                    "SELECT distinct cr.epr eprId, concat(A.sourceref,A.application) u_key, cr.product_type productType, A.* FROM ( \n" +
                    "SELECT \n" +
                    "     NULLIF(p.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(p.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , 'Delta Books' application \n" +
                    "   , split_part(NULLIF(duk.value,''), ' | ', 1) deltaanswercodeuk \n" +
                    "   , split_part(NULLIF(dus.value,''), ' | ', 1) deltaanswercodeus \n" +
                    "   , split_part (NULLIF(dan.value,''), ' | ', 1) anzpubstatus \n" +
                    "   , cast(null as date) pubdateactual \n" +
                    "   , cast(null as varchar) status \n" +
                    "   , CASE WHEN p.metadeleted = 'Y' THEN true else false END metadeleted \n" +
                    "   FROM\n" +
                    "     ((("+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product p \n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification duk ON ((p.sourceref = duk.sourceref) AND (split_part(duk.classificationcode, ' | ', 1) = 'DCADA')))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification dus ON ((p.sourceref = dus.sourceref) AND (split_part(dus.classificationcode, ' | ', 1) = 'DCAADAUS')))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification dan ON ((p.sourceref = dan.sourceref) AND (split_part(dan.classificationcode, ' | ', 1) = 'DCAANZ')))\n" +
                    "UNION    SELECT \n" +
                    "     NULLIF(sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , NULLIF(locationcode.ephcode,'') application \n" +
                    "   , null deltaanswercodeuk \n" +
                    "   , null deltaanswercodeus \n" +
                    "   , null anzpubstatus \n" +
                    "   , cast(date_parse(coalesce(NULLIF(pubdateactual,''),NULLIF(plannedpubdate,'')),'%%d-%%b-%%Y') as date) pubdateactual \n" +
                    "   , split_part (NULLIF(status,''), ' | ', 1) status \n" +
                    "   , CASE WHEN metadeleted = 'Y' THEN true else false END metadeleted \n" +
                    "   FROM\n" +
                    "     "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation \n" +
                    "   INNER JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".locationcode ON (split_part(sublocation.warehouse, ' | ', 1) = locationcode.ppmcode)\n" +
                    "   WHERE NULLIF(trim(sublocation.refkey,' '),'') IS NOT NULL\n" +
                    "UNION SELECT\n" +
                    "     NULLIF(product.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(product.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , NULLIF(locationcode.ephcode,'') application \n" +
                    "   , null deltaanswercodeuk \n" +
                    "   , null deltaanswercodeus \n" +
                    "   , null anzpubstatus \n" +
                    "   , cast(date_parse(coalesce(NULLIF(product.publishedon,''),NULLIF(product.pubdateplanned,'')),'%%d-%%b-%%Y') as date) pubdateactual \n" +
                    "   , split_part (NULLIF(product.deliverystatus,''), ' | ', 1) status \n" +
                    "   , CASE WHEN product.metadeleted = 'Y' THEN true else false END metadeleted \n" +
                    "   FROM \n" +
                    "     "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                    "   INNER JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content on product.sourceref = content.sourceref \n" +
                    "   INNER JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".locationcode ON (content.ownership = locationcode.ppmcode)\n" +
                    "   WHERE NULLIF(trim(product.refkey,' '),'') IS NOT NULL  \n" +
                    ")A \n" +
                    "INNER JOIN " + GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON \n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Product'\n" +
                    "WHERE A.metadeleted = FALSE ) where eprid in ('%s') order by eprid,u_key desc\n";

    public static String GET_AVAILABILITY_REC_CURR_DATA=
            "select u_key as UKEY " +
                    ",sourceref as SOURCEREF" +
                    ",eprid as EPRID" +
                    ",producttype as PRODUCTTYPE" +
                    ",modifiedon as MODIFIEDON" +
                    ",application as APPLICATION" +
                    ",deltaanswercodeuk as deltaanswercodeuk" +
                    ",deltaanswercodeus as deltaanswercodeus" +
                    ",anzpubstatus as anzpubstatus" +
                    ",pubdateactual as pubdateactual" +
                    ",status as status" +
                    ",metadeleted as metadeleted" +
                    " from "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_current_v " +
                    "where eprid in ('%s') order by eprid,u_key desc\n";

    public static String GET_RANDOM_MANIF_EXT_KEY_INBOUND =
            "select eprid as EPRID from (\n" +
                    "SELECT distinct cr.epr eprId, A.sourceref u_key, cr.manifestation_type, A.* FROM ( \n" +
                    "SELECT\n" +
                    "     NULLIF(m.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(m.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
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
                    "     ((((((("+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product m\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_production p ON (m.sourceref = p.sourceref))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification ukt ON ((m.sourceref = ukt.sourceref) AND (ukt.classificationcode LIKE 'MAUKT%%')))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification ust ON ((m.sourceref = ust.sourceref) AND (ust.classificationcode LIKE 'MAUST%%')))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification usd ON ((m.sourceref = usd.sourceref) AND (usd.classificationcode LIKE 'MADISC %%')))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification ukd ON ((m.sourceref = ukd.sourceref) AND (ukd.classificationcode LIKE 'MADISCEMEA%%')))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((m.sourceref = c.sourceref) AND (c.classificationcode LIKE 'DCDFC1%%')))\n" +
                    ")\n" +
                    ")A\n" +
                    "INNER JOIN " + GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Manifestation'\n" +
                    "WHERE A.metadeleted = false)order by rand() limit %s \n";

    public static String GET_MANIF_EXT_REC_INBOUND_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",uktextbookind as uktextbookind" +
                    ",ustextbookind as ustextbookind" +
                    ",usdiscountcode as usdiscountcode" +
                    ",usdiscountname as usdiscountname" +
                    ",emeadiscountcode as emeadiscountcode" +
                    ",emeadiscountname as emeadiscountname" +
                    ",trimsize as trimsize" +
                    ",weight as weight" +
                    ",commcode as commcode" +
                    ",journalprodsitecode as journalprodsitecode" +
                    ",journalissuetrimsize as journalissuetrimsize" +
                    ",warreference as warreference" +
                    " from (\n" +
                    "SELECT distinct cr.epr eprId, A.sourceref u_key, cr.manifestation_type, A.* FROM ( \n" +
                    "SELECT\n" +
                    "     NULLIF(m.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(m.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
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
                    "     ((((((("+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product m\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_production p ON (m.sourceref = p.sourceref))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification ukt ON ((m.sourceref = ukt.sourceref) AND (ukt.classificationcode LIKE 'MAUKT%%')))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification ust ON ((m.sourceref = ust.sourceref) AND (ust.classificationcode LIKE 'MAUST%%')))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification usd ON ((m.sourceref = usd.sourceref) AND (usd.classificationcode LIKE 'MADISC %%')))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification ukd ON ((m.sourceref = ukd.sourceref) AND (ukd.classificationcode LIKE 'MADISCEMEA%%')))\n" +
                    "   LEFT JOIN "+ GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((m.sourceref = c.sourceref) AND (c.classificationcode LIKE 'DCDFC1%%')))\n" +
                    ")\n" +
                    ")A\n" +
                    "INNER JOIN " + GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Manifestation'\n" +
                    "WHERE A.metadeleted = false) where eprid in ('%s') order by eprid,u_key desc\n";


    public static String GET_MANIF_EXT_REC_CURR_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",uktextbookind as uktextbookind" +
                    ",ustextbookind as ustextbookind" +
                    ",usdiscountcode as usdiscountcode" +
                    ",usdiscountname as usdiscountname" +
                    ",emeadiscountcode as emeadiscountcode" +
                    ",emeadiscountname as emeadiscountname" +
                    ",trimsize as trimsize" +
                    ",weight as weight" +
                    ",commcode as commcode" +
                    ",journalprodsitecode as journalprodsitecode" +
                    ",journalissuetrimsize as journalissuetrimsize" +
                    ",warreference as warreference" +
                    " from " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_current_v where eprid in ('%s') order by eprid,u_key desc \n";

    public static String GET_RANDOM_PAGE_COUNT_KEY_INBOUND=
            "select eprid as EPRID from (\n" +
                    "   SELECT distinct cr.epr eprId, concat(A.sourceref,A.pagecounttypecode) u_key, cr.manifestation_type, A.* FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , CASE WHEN metadeleted = 'Y' then true else false END metadeleted \n" +
                    "   , 'ROMAN' pagecounttypecode \n" +
                    "   , 'Roman' pagecounttypename \n" +
                    "   , CAST((CASE WHEN (pagesroman = '') THEN '0' ELSE pagesroman END) AS integer) pagecount \n" +
                    "   FROM\n" +
                    "     " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_production\n" +
                    "UNION  SELECT\n" +
                    "     NULLIF(sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , CASE WHEN metadeleted = 'Y' then true else false END metadeleted \n" +
                    "   , 'ARABIC' pagecounttypecode \n" +
                    "   , 'Arabic' pagecounttypename \n" +
                    "   , CAST((CASE WHEN (pagesarabic = '') THEN '0' ELSE pagesarabic END) AS integer) pagecount \n" +
                    "   FROM \n" +
                    "     "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_production\n" +
                    "UNION  SELECT\n" +
                    "     NULLIF(sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , CASE WHEN metadeleted = 'Y' then true else false END metadeleted\n" +
                    "   , 'TOTAL' pagecounttypecode \n" +
                    "   , 'Total' pagecounttypename \n" +
                    "   , (CAST((CASE WHEN (pagesroman = '') THEN '0' ELSE pagesroman END) AS integer) + CAST((CASE WHEN (pagesarabic = '') THEN '0' ELSE pagesarabic END) AS integer)) pagecount \n" +
                    "   FROM\n" +
                    "     " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_production\n" +
                    ")A\n" +
                    "INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON \n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Manifestation'\n" +
                    "WHERE A.metadeleted = FALSE ) order by rand() limit %s \n";

    public static String GET_PAGE_COUNT_EXT_REC_INBOUND_DATA=
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pagecounttypecode as pagecounttypecode" +
                    ",pagecounttypename as pagecounttypename" +
                    ",pagecount as pagecount" +
                    " from (\n" +
                    "   SELECT distinct cr.epr eprId, concat(A.sourceref,A.pagecounttypecode) u_key, cr.manifestation_type, A.* FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , CASE WHEN metadeleted = 'Y' then true else false END metadeleted \n" +
                    "   , 'ROMAN' pagecounttypecode \n" +
                    "   , 'Roman' pagecounttypename \n" +
                    "   , CAST((CASE WHEN (pagesroman = '') THEN '0' ELSE pagesroman END) AS integer) pagecount \n" +
                    "   FROM\n" +
                    "     " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_production\n" +
                    "UNION  SELECT\n" +
                    "     NULLIF(sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , CASE WHEN metadeleted = 'Y' then true else false END metadeleted \n" +
                    "   , 'ARABIC' pagecounttypecode \n" +
                    "   , 'Arabic' pagecounttypename \n" +
                    "   , CAST((CASE WHEN (pagesarabic = '') THEN '0' ELSE pagesarabic END) AS integer) pagecount \n" +
                    "   FROM \n" +
                    "     "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_production\n" +
                    "UNION  SELECT\n" +
                    "     NULLIF(sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , CASE WHEN metadeleted = 'Y' then true else false END metadeleted\n" +
                    "   , 'TOTAL' pagecounttypecode \n" +
                    "   , 'Total' pagecounttypename \n" +
                    "   , (CAST((CASE WHEN (pagesroman = '') THEN '0' ELSE pagesroman END) AS integer) + CAST((CASE WHEN (pagesarabic = '') THEN '0' ELSE pagesarabic END) AS integer)) pagecount \n" +
                    "   FROM\n" +
                    "     " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_production\n" +
                    ")A\n" +
                    "INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON \n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Manifestation'\n" +
                    "WHERE A.metadeleted = FALSE ) where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_PAGE_COUNT_REC_CURR_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pagecounttypecode as pagecounttypecode" +
                    ",pagecounttypename as pagecounttypename" +
                    ",pagecount as pagecount" +
                    " from " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_current_v where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_URL_KEY_INBOUND=
            "select eprid as EPRID from (\n" +
                    "SELECT distinct cr.epr eprId, concat(A.sourceref,A.source) u_key, cr.work_type, A.* FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , NULLIF(urltypecode.ephcode,'') urltypecode \n" +
                    "   , NULLIF(urltypecode.ephdescription,'') urltypename \n" +
                    "   , NULLIF(source,'') source\n" +
                    "   , NULLIF(name,'') name \n" +
                    "   , CASE WHEN metadeleted = 'Y' then true else false END metadeleted \n" +
                    "   FROM\n" +
                    "     (" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_extobject\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".urltypecode ON (split_part(object, ' | ', 1) = ppmcode))\n" +
                    ")A\n" +
                    "INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily f ON A.sourceref = f.sourceref and A.sourceref = f.workmasterprojectno \n" +
                    "INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
                    "f.workmasterprojectno = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Work'\n" +
                    "WHERE A.metadeleted = FALSE) order by rand() limit %s";

    public static String GET_URL_REC_INBOUND_DATA=
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",urltypecode as urltypecode" +
                    ",urltypecode as urltypecode" +
                    ",source as source" +
                    ",name as name" +
                    " from (\n" +
                    "SELECT distinct cr.epr eprId, concat(A.sourceref,A.source) u_key, cr.work_type, A.* FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , NULLIF(urltypecode.ephcode,'') urltypecode \n" +
                    "   , NULLIF(urltypecode.ephdescription,'') urltypename \n" +
                    "   , NULLIF(source,'') source\n" +
                    "   , NULLIF(name,'') name \n" +
                    "   , CASE WHEN metadeleted = 'Y' then true else false END metadeleted \n" +
                    "   FROM\n" +
                    "     (" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_extobject\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".urltypecode ON (split_part(object, ' | ', 1) = ppmcode))\n" +
                    ")A\n" +
                    "INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily f ON A.sourceref = f.sourceref and A.sourceref = f.workmasterprojectno \n" +
                    "INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
                    "f.workmasterprojectno = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Work'\n" +
                    "WHERE A.metadeleted = FALSE) where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_URL_REC_CURR_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",urltypecode as urltypecode" +
                    ",urltypecode as urltypecode" +
                    ",source as source" +
                    ",name as name" +
                    " from " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_current_v where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_WORK_EXT_REC_CURR_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",companygroup as companygroup" +
                    ",imagefileref as imagefileref" +
                    ",workmasterisbn as workmasterisbn" +
                    ",textreftrade as textreftrade" +
                    ",features as features" +
                    ",awards as awards" +
                    ",toc_long as toc_long" +
                    ",toc_short as toc_short" +
                    ",audience as audience" +
                    ",authorbyline as authorbyline" +
                    ",description as description" +
                    ",sbu as sbu" +
                    ",profitcentre as profitcentre" +
                    ",review as review" +
                    ",journalelscomind as journalelscomind" +
                    ",journalaimsscope as journalaimsscope" +
                    ",ddpeligibind as ddpeligibind" +
                    ",ptsjournalind as ptsjournalind" +
                    ",authorfeedbackind as authorfeedbackind" +
                    ",deltabusinessunit as deltabusinessunit" +
                    ",printername as printername" +
                    ",primarysitesystem as primarysitesystem" +
                    ",primarysiteacronym as primarysiteacronym" +
                    ",primarysitesupportlevel as primarysitesupportlevel" +
                    ",fulfilmentsystem as fulfilmentsystem" +
                    ",fulfilmentjournalacronym as fulfilmentjournalacronym" +
                    ",issueprodtypecode as issueprodtypecode" +
                    ",cataloguevolumesqty as cataloguevolumesqty" +
                    ",catalogueissuesqty as catalogueissuesqty" +
                    ",cataloguevolumefrom as cataloguevolumefrom" +
                    ",cataloguevolumeto as cataloguevolumeto" +
                    ",rfissuesqty as rfissuesqty" +
                    ",rftotalpagesqty as rftotalpagesqty" +
                    ",rffvi as rffvi" +
                    ",rflvi as rflvi" +
                    ",journalprevioustitle as journalprevioustitle" +
                    ",journalprimaryauthor as journalprimaryauthor" +
                    " from " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_current_v where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_WORK_EXT_KEY_INBOUND=
            "select eprid as EPRID from (\n" +
                    "SELECT distinct cr.epr eprId, A.sourceref u_key, cr.work_type, A.* FROM ( \n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(c.sourceref,'') sourceref \n" +
                    "   , NULLIF(c.companygroup,'') companygroup \n" +
                    "   , date_parse(NULLIF(c.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
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
                    "   FROM " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content c\n" +
                    "   INNER JOIN "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((c.sourceref = v.sourceref) AND (v.workmasterprojectno = v.sourceref))\n" +
                    "   LEFT JOIN (select sourceref \n" +
                    "   , max(case when split_part(classificationcode, ' | ',1) = 'PTTR' then split_part(value, ' | ', 1) end) as textreftrade\n" +
                    "   , max(case when substr(classificationcode,1,3) like 'SBU%%' then substr(classificationcode, 4, 3) end) as sbu\n" +
                    "   , max(case when substr(classificationcode,1,3) like 'SBU%%' then substr(classificationcode, (strpos(classificationcode, 'PC') + 2), 2) end) as pc\n" +
                    "   , max(case when substr(classificationcode,1,2) like 'PC%%'  then replace(split_part(classificationcode, ' | ', 1), 'PC') end) as pc2\n" +
                    "   from " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification\n" +
                    "   where (case when split_part(classificationcode, ' | ',1) in ('PTTR','DCDFAC') then true\n" +
                    "           when substr(classificationcode,1,3) like 'SBU%%' then true\n" +
                    "           when substr(classificationcode,1,2) like 'PC%%' then true\n" +
                    "           else false end) = true\n" +
                    "   group by sourceref) t ON ((t.sourceref = c.sourceref))\n" +
                    "   LEFT JOIN (select sourceref, max(case when substr(name,1,strpos(name,'|')-2)='BEBULLCPY' then text end) as bebullcpy\n" +
                    "   , max(case when substr(name,1,strpos(name,'|')-2)='SEMSBKA' then text end) as semsbka\n" +
                    "   , max(case when substr(name,1,strpos(name,'|')-2)='BETOCLNG' then text end) as betoclng\n" +
                    "   , max(case when substr(name,1,strpos(name,'|')-2)='BETOCSHT' then text end) as betocsht\n" +
                    "   , max(case when substr(name,1,strpos(name,'|')-2)='SEMKTAUD' then text end) as semktaud\n" +
                    "   , max(case when substr(name,1,strpos(name,'|')-2)='BEAUABYL' then text end) as beaubyl\n" +
                    "   from " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_text\n" +
                    "   where substr(name,1,strpos(name,'|')-2) in ('BEBULLCPY','SEMSBKA','BETOCLNG','BETOCSHT','SEMKTAUD','BEAUABYL')\n" +
                    "   group by sourceref\n" +
                    "   )  f ON (c.sourceref = f.sourceref)\n" +
                    "   LEFT JOIN (SELECT text.sourceref \n" +
                    "       , max(case when texttype.purpose = 'Review' then text.text end) as review\n" +
                    "       , max(case when texttype.purpose = 'Description' then text.text end) as description\n" +
                    "      FROM\n" +
                    "        " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_text text\n" +
                    "      INNER JOIN (\n" +
                    "         SELECT\n" +
                    "           content.sourceref \n" +
                    "         , texttypecode.texttype \n" +
                    "         , texttypecode.purpose \n" +
                    "         FROM\n" +
                    "           " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
                    "         INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".texttypecode ON ((content.objecttype = texttypecode.objecttype) AND (content.companygroup = texttypecode.companygroup))\n" +
                    "         WHERE (texttypecode.purpose in ('Review','Description'))\n" +
                    "      )  texttype ON ((text.sourceref = texttype.sourceref) AND (split_part(text.name, ' | ', 1) = texttype.texttype))\n" +
                    "      group by text.sourceref \n" +
                    "   )  d ON (c.sourceref = d.sourceref)\n" +
                    ")A\n" +
                    "INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Work'\n" +
                    "WHERE A.metadeleted = FALSE) order by rand() limit %s";

    public static String GET_WORK_EXT_INBOUND_DATA=
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",companygroup as companygroup" +
                    ",imagefileref as imagefileref" +
                    ",workmasterisbn as workmasterisbn" +
                    ",textreftrade as textreftrade" +
                    ",features as features" +
                    ",awards as awards" +
                    ",toc_long as toc_long" +
                    ",toc_short as toc_short" +
                    ",audience as audience" +
                    ",authorbyline as authorbyline" +
                    ",description as description" +
                    ",sbu as sbu" +
                    ",profitcentre as profitcentre" +
                    ",review as review" +
                    ",journalelscomind as journalelscomind" +
                    ",journalaimsscope as journalaimsscope" +
                    ",ddpeligibind as ddpeligibind" +
                    ",ptsjournalind as ptsjournalind" +
                    ",authorfeedbackind as authorfeedbackind" +
                    ",deltabusinessunit as deltabusinessunit" +
                    ",printername as printername" +
                    ",primarysitesystem as primarysitesystem" +
                    ",primarysiteacronym as primarysiteacronym" +
                    ",primarysitesupportlevel as primarysitesupportlevel" +
                    ",fulfilmentsystem as fulfilmentsystem" +
                    ",fulfilmentjournalacronym as fulfilmentjournalacronym" +
                    ",issueprodtypecode as issueprodtypecode" +
                    ",cataloguevolumesqty as cataloguevolumesqty" +
                    ",catalogueissuesqty as catalogueissuesqty" +
                    ",cataloguevolumefrom as cataloguevolumefrom" +
                    ",cataloguevolumeto as cataloguevolumeto" +
                    ",rfissuesqty as rfissuesqty" +
                    ",rftotalpagesqty as rftotalpagesqty" +
                    ",rffvi as rffvi" +
                    ",rflvi as rflvi" +
                    ",journalprevioustitle as journalprevioustitle" +
                    ",journalprimaryauthor as journalprimaryauthor" +
                    " from (\n" +
                    "SELECT distinct cr.epr eprId, A.sourceref u_key, cr.work_type, A.* FROM ( \n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(c.sourceref,'') sourceref \n" +
                    "   , NULLIF(c.companygroup,'') companygroup \n" +
                    "   , date_parse(NULLIF(c.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
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
                    "     " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content c\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((c.sourceref = v.sourceref) AND (v.workmasterprojectno = v.sourceref))\n" +
                    "   LEFT JOIN (select sourceref \n" +
                    "   , max(case when split_part(classificationcode, ' | ',1) = 'PTTR' then split_part(value, ' | ', 1) end) as textreftrade\n" +
                    "   , max(case when substr(classificationcode,1,3) like 'SBU%%' then substr(classificationcode, 4, 3) end) as sbu\n" +
                    "   , max(case when substr(classificationcode,1,3) like 'SBU%%' then substr(classificationcode, (strpos(classificationcode, 'PC') + 2), 2) end) as pc\n" +
                    "   , max(case when substr(classificationcode,1,2) like 'PC%%'  then replace(split_part(classificationcode, ' | ', 1), 'PC') end) as pc2\n" +
                    "   from " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification\n" +
                    "   where (case when split_part(classificationcode, ' | ',1) in ('PTTR','DCDFAC') then true\n" +
                    "           when substr(classificationcode,1,3) like 'SBU%%' then true\n" +
                    "           when substr(classificationcode,1,2) like 'PC%%' then true\n" +
                    "           else false end) = true\n" +
                    "   group by sourceref) t ON ((t.sourceref = c.sourceref))\n" +
                    "   LEFT JOIN (select sourceref, max(case when substr(name,1,strpos(name,'|')-2)='BEBULLCPY' then text end) as bebullcpy\n" +
                    "   , max(case when substr(name,1,strpos(name,'|')-2)='SEMSBKA' then text end) as semsbka\n" +
                    "   , max(case when substr(name,1,strpos(name,'|')-2)='BETOCLNG' then text end) as betoclng\n" +
                    "   , max(case when substr(name,1,strpos(name,'|')-2)='BETOCSHT' then text end) as betocsht\n" +
                    "   , max(case when substr(name,1,strpos(name,'|')-2)='SEMKTAUD' then text end) as semktaud\n" +
                    "   , max(case when substr(name,1,strpos(name,'|')-2)='BEAUABYL' then text end) as beaubyl\n" +
                    "   from " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_text\n" +
                    "   where substr(name,1,strpos(name,'|')-2) in ('BEBULLCPY','SEMSBKA','BETOCLNG','BETOCSHT','SEMKTAUD','BEAUABYL')\n" +
                    "   group by sourceref\n" +
                    "   )  f ON (c.sourceref = f.sourceref)\n" +
                    "   LEFT JOIN (SELECT text.sourceref \n" +
                    "       , max(case when texttype.purpose = 'Review' then text.text end) as review\n" +
                    "       , max(case when texttype.purpose = 'Description' then text.text end) as description\n" +
                    "      FROM\n" +
                    "        " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_text text\n" +
                    "      INNER JOIN (\n" +
                    "         SELECT\n" +
                    "           content.sourceref \n" +
                    "         , texttypecode.texttype \n" +
                    "         , texttypecode.purpose \n" +
                    "         FROM\n" +
                    "           " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content\n" +
                    "         INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".texttypecode ON ((content.objecttype = texttypecode.objecttype) AND (content.companygroup = texttypecode.companygroup))\n" +
                    "         WHERE (texttypecode.purpose in ('Review','Description'))\n" +
                    "      )  texttype ON ((text.sourceref = texttype.sourceref) AND (split_part(text.name, ' | ', 1) = texttype.texttype))\n" +
                    "      group by text.sourceref \n" +
                    "   )  d ON (c.sourceref = d.sourceref)\n" +
                    ")A\n" +
                    "INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Work'\n" +
                    "WHERE A.metadeleted = FALSE) where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_WORK_SUBJ_AREA_KEY_INBOUND =
            "select eprid as EPRID " +
                    " from (\n" +
                    "SELECT distinct cr.epr eprId, concat(A.sourceref,A.typecode,A.subjcode,A.subjdesc,coalesce(A.priority,'')) u_key, cr.work_type, A.* FROM ( \n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(w.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(w.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , 'PROMIS' typecode \n" +
                    "   , 'Promis' typedesc \n" +
                    "   , substr(split_part(NULLIF(c.classificationcode,''), ' | ', 1), (length(split_part(NULLIF(c.classificationcode,''), ' | ', 1)) - 4)) subjcode \n" +
                    "   , split_part (NULLIF(c.classificationcode,''), ' | ', 2) subjdesc \n" +
                    "   , NULLIF(c.priority,'') priority \n" +
                    "   , CASE WHEN w.metadeleted = 'Y' THEN true ELSE false END metadeleted \n" +
                    "   FROM\n" +
                    "     ((" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((w.sourceref = v.sourceref) AND (v.sourceref = v.workmasterprojectno)))\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((w.sourceref = c.sourceref) AND (c.classificationcode LIKE 'PROMIS%%')))\n" +
                    "UNION    SELECT DISTINCT\n" +
                    "     NULLIF(sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(modifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , NULLIF(typecode,'') typecode \n" +
                    "   , NULLIF(typedesc,'') typedesc \n" +
                    "   , NULLIF(subjcode,'') subjcode \n" +
                    "   , NULLIF(subjdesc,'') subjdesc \n" +
                    "   , NULLIF(priority,'') priority \n" +
                    "   , metadeleted \n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT DISTINCT\n" +
                    "        w.sourceref\n" +
                    "      , w.metamodifiedon modifiedon \n" +
                    "      , 'MSC' typecode \n" +
                    "      , 'Elsevier HS Major Subject Codes' typedesc \n" +
                    "      , replace(split_part(c.classificationcode, ' | ', 1), 'MSC') subjcode \n" +
                    "      , split_part(c.classificationcode, ' | ', 2) subjdesc \n" +
                    "      , c.priority \n" +
                    "      , CASE WHEN w.metadeleted = 'Y' THEN true ELSE false END metadeleted \n" +
                    "      FROM\n" +
                    "        ((" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w\n" +
                    "      INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((w.sourceref = v.sourceref) AND (v.sourceref = v.workmasterprojectno)))\n" +
                    "      INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((w.sourceref = c.sourceref) AND (c.classificationcode LIKE 'MSC%%')))\n" +
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
                    "        ((" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w\n" +
                    "      INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((w.sourceref = v.sourceref) AND (v.sourceref = v.workmasterprojectno)))\n" +
                    "      INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((w.sourceref = c.sourceref) AND (c.classificationcode LIKE 'SBU%%MSC%%')))\n" +
                    "   )\n" +
                    "UNION    SELECT DISTINCT\n" +
                    "     NULLIF(w.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(w.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    "   , 'BRASESUBDI' typecode\n" +
                    "   , 'Brazil Segmento, Subposicionamento & Disciplina' typedesc \n" +
                    "   , replace(split_part(NULLIF(c.classificationcode,''), ' | ', 1), 'BRASESUBDI') subjcode \n" +
                    "   , split_part(NULLIF(c.classificationcode,''), ' | ', 2) subjdesc \n" +
                    "   , NULLIF(c.priority,'') priority \n" +
                    "   , CASE WHEN w.metadeleted = 'Y' THEN true ELSE false END metadeleted \n" +
                    "   FROM\n" +
                    "     ((" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((w.sourceref = v.sourceref) AND (v.sourceref = v.workmasterprojectno)))\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((w.sourceref = c.sourceref) AND (c.classificationcode LIKE 'BRASESUBDI%%')))\n" +
                    ")A\n" +
                    "INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON \n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Work'\n" +
                    "WHERE A.metadeleted = FALSE) order by rand() limit %s";

    public static String GET_WORK_SUBJ_AREA_INBOUND_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",typecode as typecode" +
                    ",typedesc as typedesc" +
                    ",subjcode as subjcode" +
                    ",subjdesc as subjdesc" +
                    ",priority as priority" +
                    " from (\n" +
                    "SELECT distinct cr.epr eprId, concat(A.sourceref,A.typecode,A.subjcode,A.subjdesc,coalesce(A.priority,'')) u_key, cr.work_type, A.* FROM ( \n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(w.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(w.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , 'PROMIS' typecode \n" +
                    "   , 'Promis' typedesc \n" +
                    "   , substr(split_part(NULLIF(c.classificationcode,''), ' | ', 1), (length(split_part(NULLIF(c.classificationcode,''), ' | ', 1)) - 4)) subjcode \n" +
                    "   , split_part (NULLIF(c.classificationcode,''), ' | ', 2) subjdesc \n" +
                    "   , NULLIF(c.priority,'') priority \n" +
                    "   , CASE WHEN w.metadeleted = 'Y' THEN true ELSE false END metadeleted \n" +
                    "   FROM\n" +
                    "     ((" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((w.sourceref = v.sourceref) AND (v.sourceref = v.workmasterprojectno)))\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((w.sourceref = c.sourceref) AND (c.classificationcode LIKE 'PROMIS%%')))\n" +
                    "UNION    SELECT DISTINCT\n" +
                    "     NULLIF(sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(modifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon \n" +
                    "   , NULLIF(typecode,'') typecode \n" +
                    "   , NULLIF(typedesc,'') typedesc \n" +
                    "   , NULLIF(subjcode,'') subjcode \n" +
                    "   , NULLIF(subjdesc,'') subjdesc \n" +
                    "   , NULLIF(priority,'') priority \n" +
                    "   , metadeleted \n" +
                    "   FROM\n" +
                    "     (\n" +
                    "      SELECT DISTINCT\n" +
                    "        w.sourceref\n" +
                    "      , w.metamodifiedon modifiedon \n" +
                    "      , 'MSC' typecode \n" +
                    "      , 'Elsevier HS Major Subject Codes' typedesc \n" +
                    "      , replace(split_part(c.classificationcode, ' | ', 1), 'MSC') subjcode \n" +
                    "      , split_part(c.classificationcode, ' | ', 2) subjdesc \n" +
                    "      , c.priority \n" +
                    "      , CASE WHEN w.metadeleted = 'Y' THEN true ELSE false END metadeleted \n" +
                    "      FROM\n" +
                    "        ((" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w\n" +
                    "      INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((w.sourceref = v.sourceref) AND (v.sourceref = v.workmasterprojectno)))\n" +
                    "      INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((w.sourceref = c.sourceref) AND (c.classificationcode LIKE 'MSC%%')))\n" +
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
                    "        ((" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w\n" +
                    "      INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((w.sourceref = v.sourceref) AND (v.sourceref = v.workmasterprojectno)))\n" +
                    "      INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((w.sourceref = c.sourceref) AND (c.classificationcode LIKE 'SBU%%MSC%%')))\n" +
                    "   )\n" +
                    "UNION    SELECT DISTINCT\n" +
                    "     NULLIF(w.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(w.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    "   , 'BRASESUBDI' typecode\n" +
                    "   , 'Brazil Segmento, Subposicionamento & Disciplina' typedesc \n" +
                    "   , replace(split_part(NULLIF(c.classificationcode,''), ' | ', 1), 'BRASESUBDI') subjcode \n" +
                    "   , split_part(NULLIF(c.classificationcode,''), ' | ', 2) subjdesc \n" +
                    "   , NULLIF(c.priority,'') priority \n" +
                    "   , CASE WHEN w.metadeleted = 'Y' THEN true ELSE false END metadeleted \n" +
                    "   FROM\n" +
                    "     ((" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content w\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily v ON ((w.sourceref = v.sourceref) AND (v.sourceref = v.workmasterprojectno)))\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c ON ((w.sourceref = c.sourceref) AND (c.classificationcode LIKE 'BRASESUBDI%%')))\n" +
                    ")A\n" +
                    "INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON \n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Work'\n" +
                    "WHERE A.metadeleted = FALSE) where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_WORK_SUBJ_AREA_REC_CURR_DATA=
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",typecode as typecode" +
                    ",typedesc as typedesc" +
                    ",subjcode as subjcode" +
                    ",subjdesc as subjdesc" +
                    ",priority as priority" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_current_v where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_MANIF_RESTRICT_KEY_INBOUND =
            "select eprid as EPRID from (\n" +
                    "SELECT distinct cr.epr eprId, concat(A.sourceref, A.restrictioncode) u_key, cr.manifestation_type, A.* FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(c.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(c.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    "   , split_part(NULLIF(c.value,''), ' | ', 1) restrictioncode\n" +
                    "   , split_part(NULLIF(c.value,''), ' | ', 2) restrictionname\n" +
                    "   , CASE WHEN c.metadeleted = 'Y' THEN true ELSE false END metadeleted\n" +
                    "   FROM\n" +
                    "     (" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c)\n" +
                    "   WHERE (classificationcode LIKE 'MARESTR%%') AND (trim(value) != '')\n" +
                    ")A\n" +
                    "INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Manifestation'\n" +
                    "WHERE A.metadeleted = FALSE) order by rand() limit %s";

    public static String GET_MANIF_RESTRICT_INBOUND_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",restrictioncode as restrictioncode" +
                    ",restrictionname as restrictionname" +
                    " from (\n" +
                    "SELECT distinct cr.epr eprId, concat(A.sourceref, A.restrictioncode) u_key, cr.manifestation_type, A.* FROM (\n" +
                    "SELECT DISTINCT\n" +
                    "     NULLIF(c.sourceref,'') sourceref \n" +
                    "   , date_parse(NULLIF(c.metamodifiedon,''),'%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    "   , split_part(NULLIF(c.value,''), ' | ', 1) restrictioncode\n" +
                    "   , split_part(NULLIF(c.value,''), ' | ', 2) restrictionname\n" +
                    "   , CASE WHEN c.metadeleted = 'Y' THEN true ELSE false END metadeleted\n" +
                    "   FROM\n" +
                    "     (" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification c)\n" +
                    "   WHERE (classificationcode LIKE 'MARESTR%%') AND (trim(value) != '')\n" +
                    ")A\n" +
                    "INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON\n" +
                    "A.sourceref = cr.identifier AND \n" +
                    "cr.identifier_type = 'external_reference' AND \n" +
                    "cr.record_level = 'Manifestation'\n" +
                    "WHERE A.metadeleted = FALSE) where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_MANIF_RESTRICT_REC_CURR_DATA=
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",restrictioncode as restrictioncode" +
                    ",restrictionname as restrictionname" +
                    " from " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_current_v where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_PROD_PRICE_KEY_INBOUND=
            "select eprid as EPRID" +
                    " from (\n" +
                    "SELECT DISTINCT\n" +
                    "     cr.epr eprId\n" +
                    "   , cr.product_type\n" +
                    "   , concat(coalesce(A.sourceref, ''), coalesce(split_part(NULLIF(type, ''), ' | ', 1),''), coalesce(currency, ''), coalesce(validfrom, ''), coalesce(validto, ''), coalesce(cast(price as varchar), '')) u_key \n" +
                    "   , A.sourceref sourceref\n" +
                    "   , date_parse(NULLIF(metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    "   , NULLIF(currency, '') priceCurrency\n" +
                    "   , price priceAmount\n" +
                    "   , CAST(date_parse(NULLIF(validfrom, ''), '%%d-%%b-%%Y') AS date) priceStartDate\n" +
                    "   , CAST(date_parse(NULLIF(validto, ''), '%%d-%%b-%%Y') AS date) priceEndDate\n" +
                    "   , CAST(null AS varchar) priceRegion\n" +
                    "   , split_part(NULLIF(type, ''),' | ',1) priceCategory\n" +
                    "   , CAST(null AS varchar) priceCustomerCategory\n" +
                    "   , CAST(null AS Integer) pricePurchaseQuantity\n" +
                    "   , (CASE WHEN (metadeleted = 'Y') THEN true ELSE false END) metadeleted\n" +
                    "   FROM\n" +
                    "     (" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_pricing A\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr\n" +
                    "ON (((A.sourceref = cr.identifier) \n" +
                    "AND (cr.identifier_type = 'external_reference'))\n" +
                    "AND (cr.record_level = 'Product')))) order by rand() limit %s";

    public static String GET_PROD_PRICE_INBOUND_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",product_type as product_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pricecurrency as pricecurrency" +
                    ",priceamount as priceamount" +
                    ",pricestartdate as pricestartdate" +
                    ",priceenddate as priceenddate" +
                    ",priceregion as priceregion" +
                    ",pricecategory as pricecategory" +
                    ",pricecustomercategory as pricecustomercategory" +
                    ",pricepurchasequantity as pricepurchasequantity" +
                    " from (\n" +
                    "SELECT DISTINCT\n" +
                    "     cr.epr eprId\n" +
                    "   , cr.product_type\n" +
                    "   , concat(coalesce(A.sourceref, ''), coalesce(split_part(NULLIF(type, ''), ' | ', 1),''), coalesce(currency, ''), coalesce(validfrom, ''), coalesce(validto, ''), coalesce(cast(price as varchar), '')) u_key \n" +
                    "   , A.sourceref sourceref\n" +
                    "   , date_parse(NULLIF(metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    "   , NULLIF(currency, '') priceCurrency\n" +
                    "   , price priceAmount\n" +
                    "   , CAST(date_parse(NULLIF(validfrom, ''), '%%d-%%b-%%Y') AS date) priceStartDate\n" +
                    "   , CAST(date_parse(NULLIF(validto, ''), '%%d-%%b-%%Y') AS date) priceEndDate\n" +
                    "   , CAST(null AS varchar) priceRegion\n" +
                    "   , split_part(NULLIF(type, ''),' | ',1) priceCategory\n" +
                    "   , CAST(null AS varchar) priceCustomerCategory\n" +
                    "   , CAST(null AS Integer) pricePurchaseQuantity\n" +
                    "   , (CASE WHEN (metadeleted = 'Y') THEN true ELSE false END) metadeleted\n" +
                    "   FROM\n" +
                    "     (" +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_pricing A\n" +
                    "   INNER JOIN " +GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr\n" +
                    "ON (((A.sourceref = cr.identifier) \n" +
                    "AND (cr.identifier_type = 'external_reference'))\n" +
                    "AND (cr.record_level = 'Product')))) where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_PROD_PRICE_REC_CURR_DATA=
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",product_type as product_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pricecurrency as pricecurrency" +
                    ",priceamount as priceamount" +
                    ",pricestartdate as pricestartdate" +
                    ",priceenddate as priceenddate" +
                    ",priceregion as priceregion" +
                    ",pricecategory as pricecategory" +
                    ",pricecustomercategory as pricecustomercategory" +
                    ",pricepurchasequantity as pricepurchasequantity" +
                    " from " +GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_current_v where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_WORK_PERS_ROLE_KEY_INBOUND=
                   "select eprid as EPRID from (\n" +
                   "SELECT\n" +
                   "  concat(concat(r.sourceref, 'MARMAN'), r.personid) u_key\n" +
                   ", NULLIF(r.sourceref, '') worksourceref\n" +
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
                   ", date_parse(NULLIF(r.metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') metamodifiedon\n" +
                   ", (CASE WHEN (r.metadeleted = 'Y') THEN true ELSE false END) metadeleted\n" +
                   " FROM\n" +
                   "  ("+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_responsibilities r\n" +
                   "INNER JOIN "+GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON (((r.sourceref = cr.identifier) AND (cr.identifier_type = 'external_reference')) AND (cr.record_level = 'Work')))\n" +
                   "UNION ALL SELECT\n" +
                   "  concat(o.sourceref, rolecode.ephcode, CAST(o.businesspartnerid AS varchar)) u_key\n" +
                   ", NULLIF(o.sourceref, '') worksourceref\n" +
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
                   ", date_parse(NULLIF(o.metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                   ", (CASE WHEN (o.metadeleted = 'Y') THEN true ELSE false END) metadeleted\n" +
                   " FROM\n" +
                   "  (((((("+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originators o\n" +
                   "INNER JOIN "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(copyrightholdertype, ' | ', 1) = rolecode.ppmcode))\n" +
                   "INNER JOIN "+GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON (((o.sourceref = cr.identifier) AND (cr.identifier_type = 'external_reference')) AND (cr.record_level = 'Work')))\n" +
                   "LEFT JOIN "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originatornotes h ON (((o.businesspartnerid = h.businesspartnerid) AND (split_part(h.notestype, ' | ', 1) = 'DEG')) AND (o.sourceref = h.sourceref)))\n" +
                   "LEFT JOIN "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originatornotes a ON (((o.businesspartnerid = a.businesspartnerid) AND (split_part(a.notestype, ' | ', 1) = 'AFIL')) AND (o.sourceref = a.sourceref)))\n" +
                   "LEFT JOIN "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originatornotes n ON (((o.businesspartnerid = n.businesspartnerid) AND (split_part(n.notestype, ' | ', 1) = 'BIO')) AND (o.sourceref = n.sourceref)))\n" +
                   "JOIN "+GetBCS_ETLExtendedDLDBUser.getProdDataBase()+".gd_work_person_role gwp ON ((concat(o.sourceref, rolecode.ephcode, lower(to_hex(md5(to_utf8(concat(CAST(o.businesspartnerid AS varchar), trim(upper((CASE WHEN (isperson = 'N') THEN department ELSE firstname END))), trim(upper((CASE WHEN (isperson = 'N') THEN institution ELSE lastname END))))))))) = gwp.external_reference) AND (gwp.effective_end_date IS NULL)))\n" +
                   "WHERE (o.metadeleted = 'N')) order by rand() limit %s";


    public static String GET_WORK_PERS_ROLE_INBOUND_DATA=
                    "select eprid as EPRID" +
                    ",u_key as UKEY " +
                    ",worksourceref as worksourceref" +
                    ",personsourceref as personsourceref" +
                    ",source as source" +
                    ",work_type as work_type" +
                    ",core_reference as core_reference" +
                    ",roletype as roletype" +
                    ",rolename as rolename" +
                    ",title as title" +
                    ",person_first_name as person_first_name" +
                    ",person_family_name as person_family_name" +
                    ",email_address as email_address" +
                    ",honours as honours" +
                    ",affiliation as affiliation" +
                    ",imageurl as imageurl" +
                    ",footnotetxt as footnotetxt" +
                    ",notestxt as notestxt" +
                    ",sequence as sequence" +
                    ",groupnumber as groupnumber" +
                    ",metamodifiedon as metamodifiedon" +
                    ",metadeleted as metadeleted" +
                    " from (\n" +
                    "SELECT\n" +
                    "  concat(concat(r.sourceref, 'MARMAN'), r.personid) u_key\n" +
                    ", NULLIF(r.sourceref, '') worksourceref\n" +
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
                    ", date_parse(NULLIF(r.metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') metamodifiedon\n" +
                    ", (CASE WHEN (r.metadeleted = 'Y') THEN true ELSE false END) metadeleted\n" +
                    " FROM\n" +
                    "  ("+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_responsibilities r\n" +
                    "INNER JOIN "+GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON (((r.sourceref = cr.identifier) AND (cr.identifier_type = 'external_reference')) AND (cr.record_level = 'Work')))\n" +
                    "UNION ALL SELECT\n" +
                    "  concat(o.sourceref, rolecode.ephcode, CAST(o.businesspartnerid AS varchar)) u_key\n" +
                    ", NULLIF(o.sourceref, '') worksourceref\n" +
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
                    ", date_parse(NULLIF(o.metamodifiedon, ''), '%%d-%%b-%%Y %%H:%%i:%%s') modifiedon\n" +
                    ", (CASE WHEN (o.metadeleted = 'Y') THEN true ELSE false END) metadeleted\n" +
                    " FROM\n" +
                    "  (((((("+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originators o\n" +
                    "INNER JOIN "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".rolecode ON (split_part(copyrightholdertype, ' | ', 1) = rolecode.ppmcode))\n" +
                    "INNER JOIN "+GetBCS_ETLExtendedDLDBUser.getProductStagingDatabase()+".eph_identifier_cross_reference_v cr ON (((o.sourceref = cr.identifier) AND (cr.identifier_type = 'external_reference')) AND (cr.record_level = 'Work')))\n" +
                    "LEFT JOIN "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originatornotes h ON (((o.businesspartnerid = h.businesspartnerid) AND (split_part(h.notestype, ' | ', 1) = 'DEG')) AND (o.sourceref = h.sourceref)))\n" +
                    "LEFT JOIN "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originatornotes a ON (((o.businesspartnerid = a.businesspartnerid) AND (split_part(a.notestype, ' | ', 1) = 'AFIL')) AND (o.sourceref = a.sourceref)))\n" +
                    "LEFT JOIN "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originatornotes n ON (((o.businesspartnerid = n.businesspartnerid) AND (split_part(n.notestype, ' | ', 1) = 'BIO')) AND (o.sourceref = n.sourceref)))\n" +
                    "JOIN "+GetBCS_ETLExtendedDLDBUser.getProdDataBase()+".gd_work_person_role gwp ON ((concat(o.sourceref, rolecode.ephcode, lower(to_hex(md5(to_utf8(concat(CAST(o.businesspartnerid AS varchar), trim(upper((CASE WHEN (isperson = 'N') THEN department ELSE firstname END))), trim(upper((CASE WHEN (isperson = 'N') THEN institution ELSE lastname END))))))))) = gwp.external_reference) AND (gwp.effective_end_date IS NULL)))\n" +
                    "WHERE (o.metadeleted = 'N')) where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_WORK_PERS_ROLE_REC_CURR_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",worksourceref as worksourceref" +
                    ",personsourceref as personsourceref" +
                    ",source as source" +
                    ",work_type as work_type" +
                    ",core_reference as core_reference" +
                    ",roletype as roletype" +
                    ",rolename as rolename" +
                    ",title as title" +
                    ",person_first_name as person_first_name" +
                    ",person_family_name as person_family_name" +
                    ",email_address as email_address" +
                    ",honours as honours" +
                    ",affiliation as affiliation" +
                    ",imageurl as imageurl" +
                    ",footnotetxt as footnotetxt" +
                    ",notestxt as notestxt" +
                    ",sequence as sequence" +
                    ",groupnumber as groupnumber" +
                    ",metamodifiedon as metamodifiedon" +
                    ",metadeleted as metadeleted" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_current_v where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_AVAILABILITY_KEY_CURRENT =
            "select eprid as EPRID " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_current_v order by rand() limit %s";

    public static String GET_RANDOM_MANIF_EXT_KEY_CURRENT =
            "select eprid as EPRID " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_current_v order by rand() limit %s";

    public static String GET_RANDOM_PAGE_COUNT_KEY_CURRENT =
            "select eprid as EPRID " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_current_v order by rand() limit %s";

    public static String GET_RANDOM_URL_EXT_KEY_CURRENT =
            "select eprid as EPRID " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_current_v order by rand() limit %s";

    public static String GET_RANDOM_WORK_EXT_KEY_CURRENT =
            "select eprid as EPRID " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_current_v order by rand() limit %s";

    public static String GET_RANDOM_WORK_SUBJ_AREA_KEY_CURRENT =
            "select eprid as EPRID " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_current_v order by rand() limit %s";

    public static String GET_RANDOM_MANIF_RESTRICT_KEY_CURRENT =
            "select eprid as EPRID " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_current_v order by rand() limit %s";

    public static String GET_RANDOM_PROD_PRICE_KEY_CURRENT =
            "select eprid as EPRID " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_current_v order by rand() limit %s";

    public static String GET_RANDOM_WORK_PERSON_ROLE_KEY_CURRENT =
            "select eprid as EPRID " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_current_v order by rand() limit %s";

    public static String GET_AVAILABILTYI_REC_CURR_HIST_DATA =
            "select u_key as UKEY " +
                    ",sourceref as SOURCEREF" +
                    ",eprid as EPRID" +
                    ",producttype as PRODUCTTYPE" +
                    ",modifiedon as MODIFIEDON" +
                    ",application as APPLICATION" +
                    ",deltaanswercodeuk as deltaanswercodeuk" +
                    ",deltaanswercodeus as deltaanswercodeus" +
                    ",anzpubstatus as anzpubstatus" +
                    ",pubdateactual as pubdateactual" +
                    ",status as status" +
                    ",metadeleted as metadeleted" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_AVAILABILTYI_REC_TRANS_FILE =
            "select u_key as UKEY " +
                    ",sourceref as SOURCEREF" +
                    ",eprid as EPRID" +
                    ",producttype as PRODUCTTYPE" +
                    ",modifiedon as MODIFIEDON" +
                    ",application as APPLICATION" +
                    ",deltaanswercodeuk as deltaanswercodeuk" +
                    ",deltaanswercodeus as deltaanswercodeus" +
                    ",anzpubstatus as anzpubstatus" +
                    ",pubdateactual as pubdateactual" +
                    ",status as status" +
                    ",metadeleted as metadeleted" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_MANIF_EXT_REC_CURR_HIST_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",uktextbookind as uktextbookind" +
                    ",ustextbookind as ustextbookind" +
                    ",usdiscountcode as usdiscountcode" +
                    ",usdiscountname as usdiscountname" +
                    ",emeadiscountcode as emeadiscountcode" +
                    ",emeadiscountname as emeadiscountname" +
                    ",trimsize as trimsize" +
                    ",weight as weight" +
                    ",commcode as commcode" +
                    ",journalprodsitecode as journalprodsitecode" +
                    ",journalissuetrimsize as journalissuetrimsize" +
                    ",warreference as warreference" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_MANIF_EXT_REC_TRANS_FILE =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",uktextbookind as uktextbookind" +
                    ",ustextbookind as ustextbookind" +
                    ",usdiscountcode as usdiscountcode" +
                    ",usdiscountname as usdiscountname" +
                    ",emeadiscountcode as emeadiscountcode" +
                    ",emeadiscountname as emeadiscountname" +
                    ",trimsize as trimsize" +
                    ",weight as weight" +
                    ",commcode as commcode" +
                    ",journalprodsitecode as journalprodsitecode" +
                    ",journalissuetrimsize as journalissuetrimsize" +
                    ",warreference as warreference" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_PAGE_COUNT_REC_CURR_HIST_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pagecounttypecode as pagecounttypecode" +
                    ",pagecounttypename as pagecounttypename" +
                    ",pagecount as pagecount" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_PAGE_COUNT_REC_TRANS_FILE =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pagecounttypecode as pagecounttypecode" +
                    ",pagecounttypename as pagecounttypename" +
                    ",pagecount as pagecount" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_URL_REC_CURR_HIST_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",urltypecode as urltypecode" +
                    ",urltypecode as urltypecode" +
                    ",source as source" +
                    ",name as name" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_URL_REC_TRANS_FILE =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",urltypecode as urltypecode" +
                    ",urltypecode as urltypecode" +
                    ",source as source" +
                    ",name as name" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_WORK_EXT_REC_CURR_HIST_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",companygroup as companygroup" +
                    ",imagefileref as imagefileref" +
                    ",workmasterisbn as workmasterisbn" +
                    ",textreftrade as textreftrade" +
                    ",features as features" +
                    ",awards as awards" +
                    ",toc_long as toc_long" +
                    ",toc_short as toc_short" +
                    ",audience as audience" +
                    ",authorbyline as authorbyline" +
                    ",description as description" +
                    ",sbu as sbu" +
                    ",profitcentre as profitcentre" +
                    ",review as review" +
                    ",journalelscomind as journalelscomind" +
                    ",journalaimsscope as journalaimsscope" +
                    ",ddpeligibind as ddpeligibind" +
                    ",ptsjournalind as ptsjournalind" +
                    ",authorfeedbackind as authorfeedbackind" +
                    ",deltabusinessunit as deltabusinessunit" +
                    ",printername as printername" +
                    ",primarysitesystem as primarysitesystem" +
                    ",primarysiteacronym as primarysiteacronym" +
                    ",primarysitesupportlevel as primarysitesupportlevel" +
                    ",fulfilmentsystem as fulfilmentsystem" +
                    ",fulfilmentjournalacronym as fulfilmentjournalacronym" +
                    ",issueprodtypecode as issueprodtypecode" +
                    ",cataloguevolumesqty as cataloguevolumesqty" +
                    ",catalogueissuesqty as catalogueissuesqty" +
                    ",cataloguevolumefrom as cataloguevolumefrom" +
                    ",cataloguevolumeto as cataloguevolumeto" +
                    ",rfissuesqty as rfissuesqty" +
                    ",rftotalpagesqty as rftotalpagesqty" +
                    ",rffvi as rffvi" +
                    ",rflvi as rflvi" +
                    ",journalprevioustitle as journalprevioustitle" +
                    ",journalprimaryauthor as journalprimaryauthor" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_WORK_EXT_REC_TRANS_FILE =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",companygroup as companygroup" +
                    ",imagefileref as imagefileref" +
                    ",workmasterisbn as workmasterisbn" +
                    ",textreftrade as textreftrade" +
                    ",features as features" +
                    ",awards as awards" +
                    ",toc_long as toc_long" +
                    ",toc_short as toc_short" +
                    ",audience as audience" +
                    ",authorbyline as authorbyline" +
                    ",description as description" +
                    ",sbu as sbu" +
                    ",profitcentre as profitcentre" +
                    ",review as review" +
                    ",journalelscomind as journalelscomind" +
                    ",journalaimsscope as journalaimsscope" +
                    ",ddpeligibind as ddpeligibind" +
                    ",ptsjournalind as ptsjournalind" +
                    ",authorfeedbackind as authorfeedbackind" +
                    ",deltabusinessunit as deltabusinessunit" +
                    ",printername as printername" +
                    ",primarysitesystem as primarysitesystem" +
                    ",primarysiteacronym as primarysiteacronym" +
                    ",primarysitesupportlevel as primarysitesupportlevel" +
                    ",fulfilmentsystem as fulfilmentsystem" +
                    ",fulfilmentjournalacronym as fulfilmentjournalacronym" +
                    ",issueprodtypecode as issueprodtypecode" +
                    ",cataloguevolumesqty as cataloguevolumesqty" +
                    ",catalogueissuesqty as catalogueissuesqty" +
                    ",cataloguevolumefrom as cataloguevolumefrom" +
                    ",cataloguevolumeto as cataloguevolumeto" +
                    ",rfissuesqty as rfissuesqty" +
                    ",rftotalpagesqty as rftotalpagesqty" +
                    ",rffvi as rffvi" +
                    ",rflvi as rflvi" +
                    ",journalprevioustitle as journalprevioustitle" +
                    ",journalprimaryauthor as journalprimaryauthor" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_WORK_SUB_AREA_REC_CURR_HIST_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",typecode as typecode" +
                    ",typedesc as typedesc" +
                    ",subjcode as subjcode" +
                    ",subjdesc as subjdesc" +
                    ",priority as priority" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_WORK_SUB_AREA_REC_TRANS_FILE =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",typecode as typecode" +
                    ",typedesc as typedesc" +
                    ",subjcode as subjcode" +
                    ",subjdesc as subjdesc" +
                    ",priority as priority" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_MANIF_RESTRICT_CURR_HIST_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",restrictioncode as restrictioncode" +
                    ",restrictionname as restrictionname" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_MANIF_RESTRICT_REC_TRANS_FILE =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",restrictioncode as restrictioncode" +
                    ",restrictionname as restrictionname" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_PROD_PRICE_REC_CURR_HIST_DATA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",product_type as product_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pricecurrency as pricecurrency" +
                    ",priceamount as priceamount" +
                    ",pricestartdate as pricestartdate" +
                    ",priceenddate as priceenddate" +
                    ",priceregion as priceregion" +
                    ",pricecategory as pricecategory" +
                    ",pricecustomercategory as pricecustomercategory" +
                    ",pricepurchasequantity as pricepurchasequantity" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_PROD_PRICE_REC_TRANS_FILE =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",product_type as product_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pricecurrency as pricecurrency" +
                    ",priceamount as priceamount" +
                    ",pricestartdate as pricestartdate" +
                    ",priceenddate as priceenddate" +
                    ",priceregion as priceregion" +
                    ",pricecategory as pricecategory" +
                    ",pricecustomercategory as pricecustomercategory" +
                    ",pricepurchasequantity as pricepurchasequantity" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_WORK_PERS_ROLE_REC_CURR_HIST_DATA =
            "select eprid as EPRID " +
                    ",worksourceref as worksourceref" +
                    ",personsourceref as personsourceref" +
                    ",source as source" +
                    ",work_type as work_type" +
                    ",core_reference as core_reference" +
                    ",roletype as roletype" +
                    ",rolename as rolename" +
                    ",title as title" +
                    ",person_first_name as person_first_name" +
                    ",person_family_name as person_family_name" +
                    ",email_address as email_address" +
                    ",honours as honours" +
                    ",affiliation as affiliation" +
                    ",imageurl as imageurl" +
                    ",footnotetxt as footnotetxt" +
                    ",notestxt as notestxt" +
                    ",sequence as sequence" +
                    ",groupnumber as groupnumber" +
                    ",metamodifiedon as metamodifiedon" +
                    ",metadeleted as metadeleted" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_part where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_WORK_PERS_ROLE_REC_TRANS_FILE =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",worksourceref as worksourceref" +
                    ",personsourceref as personsourceref" +
                    ",source as source" +
                    ",work_type as work_type" +
                    ",core_reference as core_reference" +
                    ",roletype as roletype" +
                    ",rolename as rolename" +
                    ",title as title" +
                    ",person_first_name as person_first_name" +
                    ",person_family_name as person_family_name" +
                    ",email_address as email_address" +
                    ",honours as honours" +
                    ",affiliation as affiliation" +
                    ",imageurl as imageurl" +
                    ",footnotetxt as footnotetxt" +
                    ",notestxt as notestxt" +
                    ",sequence as sequence" +
                    ",groupnumber as groupnumber" +
                    //       ",metamodifiedon as metamodifiedon" +
                    ",metadeleted as metadeleted" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part where " +
                    "transform_file_ts = (select max(transform_file_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part)" +
                    " and eprid in('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_AVAILABILITY_KEY_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, application, deltaanswercodeuk, deltaanswercodeus, anzpubstatus,pubdateactual,status,metadeleted,producttype\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid,u_key, sourceref, modifiedon, application, deltaanswercodeuk, deltaanswercodeus, anzpubstatus,pubdateactual,status,metadeleted,producttype\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part dhap)where rn = 2))\n" +
                    " select u_key as UKEY " +
                    " from(\n" +
                    "    select crr.u_key,crr.eprid,crr.sourceref,crr.modifiedon,crr.application,crr.deltaanswercodeuk,crr.deltaanswercodeus,crr.anzpubstatus,crr.pubdateactual,crr.status,crr.metadeleted,crr.producttype,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.u_key,prev.eprid,prev.sourceref,prev.modifiedon,prev.application,prev.deltaanswercodeuk,prev.deltaanswercodeus,prev.anzpubstatus,prev.pubdateactual,prev.status,prev.metadeleted,prev.producttype,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key,crr.eprid,crr.sourceref,crr.modifiedon,crr.application,crr.deltaanswercodeuk,crr.deltaanswercodeus,crr.anzpubstatus,crr.pubdateactual,crr.status,crr.metadeleted,crr.producttype,'C' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "       where (coalesce(crr.eprid,'na') <> coalesce(prev.eprid,'na') or\n" +
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
                    "            coalesce (crr.producttype, 'na') <> coalesce (prev.producttype, 'na'))) order by rand() limit %s";

    public static String GET_AVAILABILTIY_REC_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, application, deltaanswercodeuk, deltaanswercodeus, anzpubstatus,pubdateactual,status,metadeleted,producttype\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid,u_key, sourceref, modifiedon, application, deltaanswercodeuk, deltaanswercodeus, anzpubstatus,pubdateactual,status,metadeleted,producttype\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_availability_extended_transform_file_history_part dhap)where rn = 2))\n" +
                    " select u_key as UKEY " +
                    ",sourceref as SOURCEREF" +
                    ",eprid as EPRID" +
                    ",producttype as PRODUCTTYPE" +
                    ",modifiedon as MODIFIEDON" +
                    ",application as APPLICATION" +
                    ",deltaanswercodeuk as deltaanswercodeuk" +
                    ",deltaanswercodeus as deltaanswercodeus" +
                    ",anzpubstatus as anzpubstatus" +
                    ",pubdateactual as pubdateactual" +
                    ",status as status" +
                    ",metadeleted as metadeleted" +
                    ",delta_mode as DELTA_MODE from(" +
                    "    select crr.u_key,crr.eprid,crr.sourceref,crr.modifiedon,crr.application,crr.deltaanswercodeuk,crr.deltaanswercodeus,crr.anzpubstatus,crr.pubdateactual,crr.status,crr.metadeleted,crr.producttype,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key,prev.eprid,prev.sourceref,prev.modifiedon,prev.application,prev.deltaanswercodeuk,prev.deltaanswercodeus,prev.anzpubstatus,prev.pubdateactual,prev.status,prev.metadeleted,prev.producttype,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key,crr.eprid,crr.sourceref,crr.modifiedon,crr.application,crr.deltaanswercodeuk,crr.deltaanswercodeus,crr.anzpubstatus,crr.pubdateactual,crr.status,crr.metadeleted,crr.producttype,'C' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "       where (coalesce(crr.eprid,'na') <> coalesce(prev.eprid,'na') or\n" +
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
                    "            coalesce (crr.producttype, 'na') <> coalesce (prev.producttype, 'na'))) where u_key in ('%s') order by u_key,eprid desc";

    public static String GET_AVAILABILTIY_REC_DELTA_CURRENT =
            " select u_key as UKEY " +
                    ",sourceref as SOURCEREF" +
                    ",eprid as EPRID" +
                    ",producttype as PRODUCTTYPE" +
                    ",modifiedon as MODIFIEDON" +
                    ",application as APPLICATION" +
                    ",deltaanswercodeuk as deltaanswercodeuk" +
                    ",deltaanswercodeus as deltaanswercodeus" +
                    ",anzpubstatus as anzpubstatus" +
                    ",pubdateactual as pubdateactual" +
                    ",status as status" +
                    ",metadeleted as metadeleted" +
                    ",delta_mode as DELTA_MODE from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_availability " +
                    "where u_key in ('%s')order by u_key,eprid desc";

    public static String GET_RANDOM_KEY_AVAILABILITY_DELTA_CURRENT_HIST =
            "select u_key as UKEY from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_availability_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_availability_part) order by rand() limit %s";

    public static String GET_AVAILABILITY_REC_DELTA_CURRENT_HIST =
            " select u_key as UKEY " +
                    ",sourceref as SOURCEREF" +
                    ",eprid as EPRID" +
                    ",producttype as PRODUCTTYPE" +
                    ",modifiedon as MODIFIEDON" +
                    ",application as APPLICATION" +
                    ",deltaanswercodeuk as deltaanswercodeuk" +
                    ",deltaanswercodeus as deltaanswercodeus" +
                    ",anzpubstatus as anzpubstatus" +
                    ",pubdateactual as pubdateactual" +
                    ",status as status" +
                    ",metadeleted as metadeleted" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_availability_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_availability_part) and u_key in ('%s') order by u_key,eprid desc";

    public static String GET_RANDOM_MANIF_EXT_KEY_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, uktextbookind, ustextbookind,usdiscountcode,usdiscountname,emeadiscountcode,emeadiscountname,trimsize,weight,commcode,journalprodsitecode,journalissuetrimsize,warreference\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, uktextbookind, ustextbookind,usdiscountcode,usdiscountname,emeadiscountcode,emeadiscountname,trimsize,weight,commcode,journalprodsitecode,journalissuetrimsize,warreference\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part dhap)where rn = 2)) \n" +
                    " select u_key as UKEY " +
                    " from(" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.manifestation_type, crr.uktextbookind, crr.ustextbookind,crr.usdiscountcode,crr.usdiscountname,crr.emeadiscountcode,crr.emeadiscountname,crr.trimsize,crr.weight,crr.commcode,crr.journalprodsitecode,crr.journalissuetrimsize,crr.warreference,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.eprid, prev.u_key, prev.sourceref, prev.modifiedon, prev.metadeleted, prev.manifestation_type, prev.uktextbookind, prev.ustextbookind,prev.usdiscountcode,prev.usdiscountname,prev.emeadiscountcode,prev.emeadiscountname,prev.trimsize,prev.weight,prev.commcode,prev.journalprodsitecode,prev.journalissuetrimsize,prev.warreference,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.manifestation_type, crr.uktextbookind, crr.ustextbookind,crr.usdiscountcode,crr.usdiscountname,crr.emeadiscountcode,crr.emeadiscountname,crr.trimsize,crr.weight,crr.commcode,crr.journalprodsitecode,crr.journalissuetrimsize,crr.warreference,'C' as delta_mode\n" +
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
                    "            coalesce (crr.warreference, 'na') <> coalesce (prev.warreference, 'na')))order by rand() limit %s";

    public static String GET_MANIF_EXT_REC_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, uktextbookind, ustextbookind,usdiscountcode,usdiscountname,emeadiscountcode,emeadiscountname,trimsize,weight,commcode,journalprodsitecode,journalissuetrimsize,warreference\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, uktextbookind, ustextbookind,usdiscountcode,usdiscountname,emeadiscountcode,emeadiscountname,trimsize,weight,commcode,journalprodsitecode,journalissuetrimsize,warreference\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_extended_transform_file_history_part dhap)where rn = 2)) \n" +
                    " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",uktextbookind as uktextbookind" +
                    ",ustextbookind as ustextbookind" +
                    ",usdiscountcode as usdiscountcode" +
                    ",usdiscountname as usdiscountname" +
                    ",emeadiscountcode as emeadiscountcode" +
                    ",emeadiscountname as emeadiscountname" +
                    ",trimsize as trimsize" +
                    ",weight as weight" +
                    ",commcode as commcode" +
                    ",journalprodsitecode as journalprodsitecode" +
                    ",journalissuetrimsize as journalissuetrimsize" +
                    ",warreference as warreference" +
                    ",delta_mode as DELTA_MODE from(" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.manifestation_type, crr.uktextbookind, crr.ustextbookind,crr.usdiscountcode,crr.usdiscountname,crr.emeadiscountcode,crr.emeadiscountname,crr.trimsize,crr.weight,crr.commcode,crr.journalprodsitecode,crr.journalissuetrimsize,crr.warreference,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.eprid, prev.u_key, prev.sourceref, prev.modifiedon, prev.metadeleted, prev.manifestation_type, prev.uktextbookind, prev.ustextbookind,prev.usdiscountcode,prev.usdiscountname,prev.emeadiscountcode,prev.emeadiscountname,prev.trimsize,prev.weight,prev.commcode,prev.journalprodsitecode,prev.journalissuetrimsize,prev.warreference,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.manifestation_type, crr.uktextbookind, crr.ustextbookind,crr.usdiscountcode,crr.usdiscountname,crr.emeadiscountcode,crr.emeadiscountname,crr.trimsize,crr.weight,crr.commcode,crr.journalprodsitecode,crr.journalissuetrimsize,crr.warreference,'C' as delta_mode\n" +
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
                    "            coalesce (crr.warreference, 'na') <> coalesce (prev.warreference, 'na')))where u_key in ('%s') order by u_key,eprid desc";

    public static String GET_RANDOM_KEY_MANIF_EXT_DELTA_CURRENT_HIST =
            "select u_key as UKEY from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_manifestation_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_manifestation_part) order by rand() limit %s";

    public static String GET_MANIF_EXT_REC_DELTA_CURRENT_HIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",uktextbookind as uktextbookind" +
                    ",ustextbookind as ustextbookind" +
                    ",usdiscountcode as usdiscountcode" +
                    ",usdiscountname as usdiscountname" +
                    ",emeadiscountcode as emeadiscountcode" +
                    ",emeadiscountname as emeadiscountname" +
                    ",trimsize as trimsize" +
                    ",weight as weight" +
                    ",commcode as commcode" +
                    ",journalprodsitecode as journalprodsitecode" +
                    ",journalissuetrimsize as journalissuetrimsize" +
                    ",warreference as warreference" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_manifestation_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_manifestation_part) and u_key in ('%s') order by u_key,eprid desc";

    public static String GET_MANIF_EXT_REC_DELTA_CURRENT =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",uktextbookind as uktextbookind" +
                    ",ustextbookind as ustextbookind" +
                    ",usdiscountcode as usdiscountcode" +
                    ",usdiscountname as usdiscountname" +
                    ",emeadiscountcode as emeadiscountcode" +
                    ",emeadiscountname as emeadiscountname" +
                    ",trimsize as trimsize" +
                    ",weight as weight" +
                    ",commcode as commcode" +
                    ",journalprodsitecode as journalprodsitecode" +
                    ",journalissuetrimsize as journalissuetrimsize" +
                    ",warreference as warreference" +
                    ",delta_mode as DELTA_MODE from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation" +
                    " where u_key in ('%s') order by u_key,eprid desc";

    public static String GET_RANDOM_URL_KEY_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, urltypecode, urltypename, source, name\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, urltypecode, urltypename, source, name\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part dhap)where rn = 2))\n" +
                    " select u_key as UKEY " +
                    " from(" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.work_type, crr.urltypecode, crr.urltypename, crr.source, crr.name,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.eprid, prev.u_key, prev.sourceref, prev.modifiedon, prev.metadeleted, prev.work_type, prev.urltypecode, prev.urltypename, prev.source, prev.name,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.work_type, crr.urltypecode, crr.urltypename, crr.source, crr.name,'C' as delta_mode\n" +
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
                    "            coalesce (crr.name, 'na') <> coalesce (prev.name, 'na')))order by rand() limit %s";

    public static String GET_URL_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, urltypecode, urltypename, source, name\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, urltypecode, urltypename, source, name\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_url_extended_transform_file_history_part dhap)where rn = 2))\n" +
                    " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",urltypecode as urltypecode" +
                    ",urltypecode as urltypecode" +
                    ",source as source" +
                    ",name as name" +
                    ",delta_mode as DELTA_MODE from(" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.work_type, crr.urltypecode, crr.urltypename, crr.source, crr.name,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.eprid, prev.u_key, prev.sourceref, prev.modifiedon, prev.metadeleted, prev.work_type, prev.urltypecode, prev.urltypename, prev.source, prev.name,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.work_type, crr.urltypecode, crr.urltypename, crr.source, crr.name,'C' as delta_mode\n" +
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
                    "            coalesce (crr.name, 'na') <> coalesce (prev.name, 'na')))where u_key in ('%s') order by u_key,eprid desc";


    public static String GET_RANDOM_KEY_PAGE_COUNT_DELTA_CURRENT_HIST =
            "select u_key as UKEY from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_page_count_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_page_count_part) order by rand() limit %s";

    public static String GET_PAGE_COUNT_REC_DELTA_CURR_HIST =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pagecounttypecode as pagecounttypecode" +
                    ",pagecounttypename as pagecounttypename" +
                    ",pagecount as pagecount" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_page_count_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_page_count_part) and u_key in ('%s') order by u_key,eprid desc";

    public static String GET_RANDOM_KEY_URL_DELTA_CURRENT_HIST =
            "select u_key as UKEY from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_url_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_url_part) order by rand() limit %s";

    public static String GET_URL_REC_DELTA_CURRENT_HIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",urltypecode as urltypecode" +
                    ",urltypecode as urltypecode" +
                    ",source as source" +
                    ",name as name" +
                    ",delta_mode as DELTA_MODE"+
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_url_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_url_part) and u_key in ('%s') order by u_key,eprid desc";

    public static String GET_URL_REC_DELTA_CURRENT =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",urltypecode as urltypecode" +
                    ",urltypecode as urltypecode" +
                    ",source as source" +
                    ",name as name" +
                    ",delta_mode as DELTA_MODE from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_url where" +
                    " u_key in ('%s') order by u_key,eprid desc";

    public static String GET_PAGE_COUNT_REC_DELTA_CURRENT =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pagecounttypecode as pagecounttypecode" +
                    ",pagecounttypename as pagecounttypename" +
                    ",pagecount as pagecount" +
                    ",delta_mode as DELTA_MODE from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_page_count" +
                    " where u_key in ('%s') order by u_key,eprid desc";

    public static String GET_RANDOM_PAGE_COUNT_KEY_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, pagecounttypecode, pagecounttypename,pagecount\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, pagecounttypecode, pagecounttypename,pagecount\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part dhap)where rn = 2)) \n" +
                    "select u_key as UKEY " +
                    " from(" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.manifestation_type, crr.pagecounttypecode, crr.pagecounttypename,crr.pagecount,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.eprid, prev.u_key, prev.sourceref, prev.modifiedon, prev.metadeleted, prev.manifestation_type, prev.pagecounttypecode, prev.pagecounttypename,prev.pagecount,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.manifestation_type, crr.pagecounttypecode,crr.pagecounttypename,crr.pagecount,'C' as delta_mode\n" +
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
                    "            coalesce (crr.pagecount, 0) <> coalesce (prev.pagecount, 0))) order by rand() limit %s";

    public static String GET_PAGE_COUNT_REC_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, pagecounttypecode, pagecounttypename,pagecount\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, pagecounttypecode, pagecounttypename,pagecount\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_page_count_extended_transform_file_history_part dhap)where rn = 2)) \n" +
                    "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pagecounttypecode as pagecounttypecode" +
                    ",pagecounttypename as pagecounttypename" +
                    ",pagecount as pagecount" +
                    ",delta_mode as DELTA_MODE from(" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.manifestation_type, crr.pagecounttypecode, crr.pagecounttypename,crr.pagecount,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.eprid, prev.u_key, prev.sourceref, prev.modifiedon, prev.metadeleted, prev.manifestation_type, prev.pagecounttypecode, prev.pagecounttypename,prev.pagecount,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.manifestation_type, crr.pagecounttypecode,crr.pagecounttypename,crr.pagecount,'C' as delta_mode\n" +
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
                    "            coalesce (crr.pagecount, 0) <> coalesce (prev.pagecount, 0)))where u_key in ('%s') order by u_key,eprid desc";


    public static String GET_RANDOM_WORK_EXT_KEY_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, companygroup, imagefileref, workmasterisbn, textreftrade, features,awards,toc_long,toc_short,audience,authorbyline,description,sbu,profitcentre,review,journalelscomind,journalaimsscope,ddpeligibind,ptsjournalind,authorfeedbackind,deltabusinessunit,printername,primarysitesystem,primarysiteacronym,primarysitesupportlevel,fulfilmentsystem,fulfilmentjournalacronym,issueprodtypecode,cataloguevolumesqty,catalogueissuesqty,cataloguevolumefrom,cataloguevolumeto,rfissuesqty,rftotalpagesqty,rffvi,rflvi,journalprevioustitle,journalprimaryauthor\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, companygroup, imagefileref, workmasterisbn, textreftrade, features,awards,toc_long,toc_short,audience,authorbyline,description,sbu,profitcentre,review,journalelscomind,journalaimsscope,ddpeligibind,ptsjournalind,authorfeedbackind,deltabusinessunit,printername,primarysitesystem,primarysiteacronym,primarysitesupportlevel,fulfilmentsystem,fulfilmentjournalacronym,issueprodtypecode,cataloguevolumesqty,catalogueissuesqty,cataloguevolumefrom,cataloguevolumeto,rfissuesqty,rftotalpagesqty,rffvi,rflvi,journalprevioustitle,journalprimaryauthor\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    " select u_key as UKEY " +
                    " from( \n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted,crr.work_type,crr.companygroup,crr.imagefileref, crr.workmasterisbn,crr.textreftrade, crr.features,crr.awards,crr.toc_long,crr.toc_short,crr.audience,crr.authorbyline,crr.description,crr.sbu,crr.profitcentre,crr.review,crr.journalelscomind,crr.journalaimsscope,crr.ddpeligibind,crr.ptsjournalind,crr.authorfeedbackind,crr.deltabusinessunit,crr.printername,crr.primarysitesystem,crr.primarysiteacronym,crr.primarysitesupportlevel,crr.fulfilmentsystem,crr.fulfilmentjournalacronym,crr.issueprodtypecode,crr.cataloguevolumesqty,crr.catalogueissuesqty,crr.cataloguevolumefrom,crr.cataloguevolumeto,crr.rfissuesqty,crr.rftotalpagesqty,crr.rffvi,crr.rflvi,crr.journalprevioustitle,crr.journalprimaryauthor,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.eprid, prev.u_key, prev.sourceref, prev.modifiedon, prev.metadeleted,prev.work_type,prev.companygroup,prev.imagefileref, prev.workmasterisbn,prev.textreftrade, prev.features,prev.awards,prev.toc_long,prev.toc_short,prev.audience,prev.authorbyline,prev.description,prev.sbu,prev.profitcentre,prev.review,prev.journalelscomind,prev.journalaimsscope,prev.ddpeligibind,prev.ptsjournalind,prev.authorfeedbackind,prev.deltabusinessunit,prev.printername,prev.primarysitesystem,prev.primarysiteacronym,prev.primarysitesupportlevel,prev.fulfilmentsystem,prev.fulfilmentjournalacronym,prev.issueprodtypecode,prev.cataloguevolumesqty,prev.catalogueissuesqty,prev.cataloguevolumefrom,prev.cataloguevolumeto,prev.rfissuesqty,prev.rftotalpagesqty,prev.rffvi,prev.rflvi,prev.journalprevioustitle,prev.journalprimaryauthor,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted,crr.work_type,crr.companygroup,crr.imagefileref, crr.workmasterisbn,crr.textreftrade, crr.features,crr.awards,crr.toc_long,crr.toc_short,crr.audience,crr.authorbyline,crr.description,crr.sbu,crr.profitcentre,crr.review,crr.journalelscomind,crr.journalaimsscope,crr.ddpeligibind,crr.ptsjournalind,crr.authorfeedbackind,crr.deltabusinessunit,crr.printername,crr.primarysitesystem,crr.primarysiteacronym,crr.primarysitesupportlevel,crr.fulfilmentsystem,crr.fulfilmentjournalacronym,crr.issueprodtypecode,crr.cataloguevolumesqty,crr.catalogueissuesqty,crr.cataloguevolumefrom,crr.cataloguevolumeto,crr.rfissuesqty,crr.rftotalpagesqty,crr.rffvi,crr.rflvi,crr.journalprevioustitle,crr.journalprimaryauthor,'C' as delta_mode\n" +
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
                    "            coalesce (crr.journalprimaryauthor, 'na') <> coalesce (prev.journalprimaryauthor, 'na'))) order by rand() limit %s";


    public static String GET_WORK_EXT_REC_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, companygroup, imagefileref, workmasterisbn, textreftrade, features,awards,toc_long,toc_short,audience,authorbyline,description,sbu,profitcentre,review,journalelscomind,journalaimsscope,ddpeligibind,ptsjournalind,authorfeedbackind,deltabusinessunit,printername,primarysitesystem,primarysiteacronym,primarysitesupportlevel,fulfilmentsystem,fulfilmentjournalacronym,issueprodtypecode,cataloguevolumesqty,catalogueissuesqty,cataloguevolumefrom,cataloguevolumeto,rfissuesqty,rftotalpagesqty,rffvi,rflvi,journalprevioustitle,journalprimaryauthor\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, companygroup, imagefileref, workmasterisbn, textreftrade, features,awards,toc_long,toc_short,audience,authorbyline,description,sbu,profitcentre,review,journalelscomind,journalaimsscope,ddpeligibind,ptsjournalind,authorfeedbackind,deltabusinessunit,printername,primarysitesystem,primarysiteacronym,primarysitesupportlevel,fulfilmentsystem,fulfilmentjournalacronym,issueprodtypecode,cataloguevolumesqty,catalogueissuesqty,cataloguevolumefrom,cataloguevolumeto,rfissuesqty,rftotalpagesqty,rffvi,rflvi,journalprevioustitle,journalprimaryauthor\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",companygroup as companygroup" +
                    ",imagefileref as imagefileref" +
                    ",workmasterisbn as workmasterisbn" +
                    ",textreftrade as textreftrade" +
                    ",features as features" +
                    ",awards as awards" +
                    ",toc_long as toc_long" +
                    ",toc_short as toc_short" +
                    ",audience as audience" +
                    ",authorbyline as authorbyline" +
                    ",description as description" +
                    ",sbu as sbu" +
                    ",profitcentre as profitcentre" +
                    ",review as review" +
                    ",journalelscomind as journalelscomind" +
                    ",journalaimsscope as journalaimsscope" +
                    ",ddpeligibind as ddpeligibind" +
                    ",ptsjournalind as ptsjournalind" +
                    ",authorfeedbackind as authorfeedbackind" +
                    ",deltabusinessunit as deltabusinessunit" +
                    ",printername as printername" +
                    ",primarysitesystem as primarysitesystem" +
                    ",primarysiteacronym as primarysiteacronym" +
                    ",primarysitesupportlevel as primarysitesupportlevel" +
                    ",fulfilmentsystem as fulfilmentsystem" +
                    ",fulfilmentjournalacronym as fulfilmentjournalacronym" +
                    ",issueprodtypecode as issueprodtypecode" +
                    ",cataloguevolumesqty as cataloguevolumesqty" +
                    ",catalogueissuesqty as catalogueissuesqty" +
                    ",cataloguevolumefrom as cataloguevolumefrom" +
                    ",cataloguevolumeto as cataloguevolumeto" +
                    ",rfissuesqty as rfissuesqty" +
                    ",rftotalpagesqty as rftotalpagesqty" +
                    ",rffvi as rffvi" +
                    ",rflvi as rflvi" +
                    ",journalprevioustitle as journalprevioustitle" +
                    ",journalprimaryauthor as journalprimaryauthor" +
                    ",delta_mode as DELTA_MODE" +
                    " from( \n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted,crr.work_type,crr.companygroup,crr.imagefileref, crr.workmasterisbn,crr.textreftrade, crr.features,crr.awards,crr.toc_long,crr.toc_short,crr.audience,crr.authorbyline,crr.description,crr.sbu,crr.profitcentre,crr.review,crr.journalelscomind,crr.journalaimsscope,crr.ddpeligibind,crr.ptsjournalind,crr.authorfeedbackind,crr.deltabusinessunit,crr.printername,crr.primarysitesystem,crr.primarysiteacronym,crr.primarysitesupportlevel,crr.fulfilmentsystem,crr.fulfilmentjournalacronym,crr.issueprodtypecode,crr.cataloguevolumesqty,crr.catalogueissuesqty,crr.cataloguevolumefrom,crr.cataloguevolumeto,crr.rfissuesqty,crr.rftotalpagesqty,crr.rffvi,crr.rflvi,crr.journalprevioustitle,crr.journalprimaryauthor,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.eprid, prev.u_key, prev.sourceref, prev.modifiedon, prev.metadeleted,prev.work_type,prev.companygroup,prev.imagefileref, prev.workmasterisbn,prev.textreftrade, prev.features,prev.awards,prev.toc_long,prev.toc_short,prev.audience,prev.authorbyline,prev.description,prev.sbu,prev.profitcentre,prev.review,prev.journalelscomind,prev.journalaimsscope,prev.ddpeligibind,prev.ptsjournalind,prev.authorfeedbackind,prev.deltabusinessunit,prev.printername,prev.primarysitesystem,prev.primarysiteacronym,prev.primarysitesupportlevel,prev.fulfilmentsystem,prev.fulfilmentjournalacronym,prev.issueprodtypecode,prev.cataloguevolumesqty,prev.catalogueissuesqty,prev.cataloguevolumefrom,prev.cataloguevolumeto,prev.rfissuesqty,prev.rftotalpagesqty,prev.rffvi,prev.rflvi,prev.journalprevioustitle,prev.journalprimaryauthor,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted,crr.work_type,crr.companygroup,crr.imagefileref, crr.workmasterisbn,crr.textreftrade, crr.features,crr.awards,crr.toc_long,crr.toc_short,crr.audience,crr.authorbyline,crr.description,crr.sbu,crr.profitcentre,crr.review,crr.journalelscomind,crr.journalaimsscope,crr.ddpeligibind,crr.ptsjournalind,crr.authorfeedbackind,crr.deltabusinessunit,crr.printername,crr.primarysitesystem,crr.primarysiteacronym,crr.primarysitesupportlevel,crr.fulfilmentsystem,crr.fulfilmentjournalacronym,crr.issueprodtypecode,crr.cataloguevolumesqty,crr.catalogueissuesqty,crr.cataloguevolumefrom,crr.cataloguevolumeto,crr.rfissuesqty,crr.rftotalpagesqty,crr.rffvi,crr.rflvi,crr.journalprevioustitle,crr.journalprimaryauthor,'C' as delta_mode\n" +
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
                    "            coalesce (crr.journalprimaryauthor, 'na') <> coalesce (prev.journalprimaryauthor, 'na'))) where u_key in ('%s') order by u_key,eprid desc";

    public static String GET_RANDOM_KEY_WORK_EXT_DELTA_CURRENT_HIST =
            "select u_key as UKEY from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_part) order by rand() limit %s";

    public static String GET_WORK_EXT_REC_DELTA_CURRENT_HIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",companygroup as companygroup" +
                    ",imagefileref as imagefileref" +
                    ",workmasterisbn as workmasterisbn" +
                    ",textreftrade as textreftrade" +
                    ",features as features" +
                    ",awards as awards" +
                    ",toc_long as toc_long" +
                    ",toc_short as toc_short" +
                    ",audience as audience" +
                    ",authorbyline as authorbyline" +
                    ",description as description" +
                    ",sbu as sbu" +
                    ",profitcentre as profitcentre" +
                    ",review as review" +
                    ",journalelscomind as journalelscomind" +
                    ",journalaimsscope as journalaimsscope" +
                    ",ddpeligibind as ddpeligibind" +
                    ",ptsjournalind as ptsjournalind" +
                    ",authorfeedbackind as authorfeedbackind" +
                    ",deltabusinessunit as deltabusinessunit" +
                    ",printername as printername" +
                    ",primarysitesystem as primarysitesystem" +
                    ",primarysiteacronym as primarysiteacronym" +
                    ",primarysitesupportlevel as primarysitesupportlevel" +
                    ",fulfilmentsystem as fulfilmentsystem" +
                    ",fulfilmentjournalacronym as fulfilmentjournalacronym" +
                    ",issueprodtypecode as issueprodtypecode" +
                    ",cataloguevolumesqty as cataloguevolumesqty" +
                    ",catalogueissuesqty as catalogueissuesqty" +
                    ",cataloguevolumefrom as cataloguevolumefrom" +
                    ",cataloguevolumeto as cataloguevolumeto" +
                    ",rfissuesqty as rfissuesqty" +
                    ",rftotalpagesqty as rftotalpagesqty" +
                    ",rffvi as rffvi" +
                    ",rflvi as rflvi" +
                    ",journalprevioustitle as journalprevioustitle" +
                    ",journalprimaryauthor as journalprimaryauthor" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_part) and u_key in ('%s') order by u_key,eprid desc";

    public static String GET_WORK_EXT_REC_DELTA_CURRENT =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",companygroup as companygroup" +
                    ",imagefileref as imagefileref" +
                    ",workmasterisbn as workmasterisbn" +
                    ",textreftrade as textreftrade" +
                    ",features as features" +
                    ",awards as awards" +
                    ",toc_long as toc_long" +
                    ",toc_short as toc_short" +
                    ",audience as audience" +
                    ",authorbyline as authorbyline" +
                    ",description as description" +
                    ",sbu as sbu" +
                    ",profitcentre as profitcentre" +
                    ",review as review" +
                    ",journalelscomind as journalelscomind" +
                    ",journalaimsscope as journalaimsscope" +
                    ",ddpeligibind as ddpeligibind" +
                    ",ptsjournalind as ptsjournalind" +
                    ",authorfeedbackind as authorfeedbackind" +
                    ",deltabusinessunit as deltabusinessunit" +
                    ",printername as printername" +
                    ",primarysitesystem as primarysitesystem" +
                    ",primarysiteacronym as primarysiteacronym" +
                    ",primarysitesupportlevel as primarysitesupportlevel" +
                    ",fulfilmentsystem as fulfilmentsystem" +
                    ",fulfilmentjournalacronym as fulfilmentjournalacronym" +
                    ",issueprodtypecode as issueprodtypecode" +
                    ",cataloguevolumesqty as cataloguevolumesqty" +
                    ",catalogueissuesqty as catalogueissuesqty" +
                    ",cataloguevolumefrom as cataloguevolumefrom" +
                    ",cataloguevolumeto as cataloguevolumeto" +
                    ",rfissuesqty as rfissuesqty" +
                    ",rftotalpagesqty as rftotalpagesqty" +
                    ",rffvi as rffvi" +
                    ",rflvi as rflvi" +
                    ",journalprevioustitle as journalprevioustitle" +
                    ",journalprimaryauthor as journalprimaryauthor" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work where u_key in ('%s') order by u_key,eprid desc";


    public static String GET_RANDOM_WORK_SUB_AREA_KEY_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, typecode, typedesc, subjcode, subjdesc, priority\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, typecode, typedesc, subjcode, subjdesc, priority\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    "select u_key as UKEY from( \n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.work_type, crr.typecode, crr.typedesc, crr.subjcode, crr.subjdesc, crr.priority,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.eprid, prev.u_key, prev.sourceref, prev.modifiedon, prev.metadeleted, prev.work_type, prev.typecode, prev.typedesc, prev.subjcode, prev.subjdesc, prev.priority,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.work_type, crr.typecode, crr.typedesc, crr.subjcode, crr.subjdesc, crr.priority,'C' as delta_mode\n" +
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
                    "            coalesce (crr.priority, 'na') <> coalesce (prev.priority, 'na'))) order by rand() limit %s";

    public static String GET_WORK_SUBJ_AREA_REC_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, typecode, typedesc, subjcode, subjdesc, priority\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, work_type, typecode, typedesc, subjcode, subjdesc, priority\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_subject_area_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",typecode as typecode" +
                    ",typedesc as typedesc" +
                    ",subjcode as subjcode" +
                    ",subjdesc as subjdesc" +
                    ",priority as priority" +
                    ",delta_mode as DELTA_MODE" +
                    " from( \n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.work_type, crr.typecode, crr.typedesc, crr.subjcode, crr.subjdesc, crr.priority,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.eprid, prev.u_key, prev.sourceref, prev.modifiedon, prev.metadeleted, prev.work_type, prev.typecode, prev.typedesc, prev.subjcode, prev.subjdesc, prev.priority,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.work_type, crr.typecode, crr.typedesc, crr.subjcode, crr.subjdesc, crr.priority,'C' as delta_mode\n" +
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
                    "            coalesce (crr.priority, 'na') <> coalesce (prev.priority, 'na'))) where u_key in ('%s') order by u_key,eprid desc";

    public static String GET_RANDOM_KEY_WORK_SUB_AREA_DELTA_CURRENT_HIST =
            "select u_key as UKEY from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_subject_area_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_subject_area_part) order by rand() limit %s";

    public static String GET_WORK_SUBJ_AREA_REC_DELTA_CURRENT_HIST =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",typecode as typecode" +
                    ",typedesc as typedesc" +
                    ",subjcode as subjcode" +
                    ",subjdesc as subjdesc" +
                    ",priority as priority" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_subject_area_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_subject_area_part) and u_key in ('%s') order by u_key,eprid desc";

    public static String GET_WORK_SUB_AREA_REC_DELTA_CURRENT =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",typecode as typecode" +
                    ",typedesc as typedesc" +
                    ",subjcode as subjcode" +
                    ",subjdesc as subjdesc" +
                    ",priority as priority" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_subject_area where u_key in ('%s') order by u_key,eprid desc";

    public static String GET_RANDOM_MANIF_RESTRICT_KEY_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, restrictioncode, restrictionname\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, restrictioncode, restrictionname\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    "select u_key as UKEY \n" +
                    "from( \n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.manifestation_type, crr.restrictioncode, crr.restrictionname,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.eprid, prev.u_key, prev.sourceref, prev.modifiedon, prev.metadeleted, prev.manifestation_type, prev.restrictioncode, prev.restrictionname,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.manifestation_type, crr.restrictioncode, crr.restrictionname,'C' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.eprid,'na') <> coalesce(prev.eprid,'na') or\n" +
                    "            coalesce(crr.u_key,'na') <> coalesce(prev.u_key,'na') or\n" +
                    "            coalesce (crr.manifestation_type, 'na') <> coalesce (prev.manifestation_type, 'na') or\n" +
                    "            coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false) or \n" +
                    "            coalesce (crr.sourceref, 'na') <> coalesce (prev.sourceref, 'na') or\n" +
                    "            coalesce (crr.restrictioncode, 'na') <> coalesce (prev.restrictioncode, 'na') or \n" +
                    "            coalesce (crr.restrictionname, 'na') <> coalesce (prev.restrictionname , 'na'))) order by rand() limit %s";

    public static String GET_MANIF_RESTRICT_REC_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, restrictioncode, restrictionname\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, manifestation_type, restrictioncode, restrictionname\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_restrictions_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",restrictioncode as restrictioncode" +
                    ",restrictionname as restrictionname" +
                    ",delta_mode as DELTA_MODE" +
                    " from( \n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.manifestation_type, crr.restrictioncode, crr.restrictionname,'I' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.eprid, prev.u_key, prev.sourceref, prev.modifiedon, prev.metadeleted, prev.manifestation_type, prev.restrictioncode, prev.restrictionname,'D' as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid, crr.u_key, crr.sourceref, crr.modifiedon, crr.metadeleted, crr.manifestation_type, crr.restrictioncode, crr.restrictionname,'C' as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where (coalesce(crr.eprid,'na') <> coalesce(prev.eprid,'na') or\n" +
                    "            coalesce(crr.u_key,'na') <> coalesce(prev.u_key,'na') or\n" +
                    "            coalesce (crr.manifestation_type, 'na') <> coalesce (prev.manifestation_type, 'na') or\n" +
                    "            coalesce (crr.modifiedon, current_date) <> coalesce (prev.modifiedon, current_date) or\n" +
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false) or \n" +
                    "            coalesce (crr.sourceref, 'na') <> coalesce (prev.sourceref, 'na') or\n" +
                    "            coalesce (crr.restrictioncode, 'na') <> coalesce (prev.restrictioncode, 'na') or \n" +
                    "            coalesce (crr.restrictionname, 'na') <> coalesce (prev.restrictionname , 'na'))) where u_key in ('%s') order by eprid,eprid desc";

    public static String GET_MANIF_RESTRICT_REC_DIFF_DELTA_CURRENT =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",restrictioncode as restrictioncode" +
                    ",restrictionname as restrictionname" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation_restrictions where u_key in ('%s') order by u_key,eprid desc";

    public static String GET_RANDOM_KEY_MANIF_RESTRICT_DELTA_CURRENT_HIST =
            "select u_key as UKEY" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_manifestation_restrictions_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_manifestation_restrictions_part) order by rand() limit %s";

    public static String GET_MANIF_RESTRICT_REC_DELTA_CURRENT_HIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",restrictioncode as restrictioncode" +
                    ",restrictionname as restrictionname" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_manifestation_restrictions_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_manifestation_restrictions_part) and u_key in ('%s') order by u_key,eprid desc";


    public static String GET_RANDOM_PROD_PRICE_KEY_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, product_type, pricecurrency, priceamount, pricestartdate, priceenddate, priceregion,pricecategory,pricecustomercategory,pricepurchasequantity\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, product_type, pricecurrency, priceamount, pricestartdate, priceenddate, priceregion,pricecategory,pricecustomercategory,pricepurchasequantity\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    "select u_key as UKEY from( \n" +
                    "    select crr.eprid,crr.u_key,crr.sourceref,crr.modifiedon,crr.metadeleted,crr.product_type,crr.pricecurrency,crr.priceamount,crr.pricestartdate,crr.priceenddate,crr.priceregion,crr.pricecategory,crr.pricecustomercategory,crr.pricepurchasequantity,'I'as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.eprid,prev.u_key,prev.sourceref,prev.modifiedon,prev.metadeleted,prev.product_type,prev.pricecurrency,prev.priceamount,prev.pricestartdate,prev.priceenddate,prev.priceregion,prev.pricecategory,prev.pricecustomercategory,prev.pricepurchasequantity,'D'as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid,crr.u_key,crr.sourceref,crr.modifiedon,crr.metadeleted,crr.product_type,crr.pricecurrency,crr.priceamount,crr.pricestartdate,crr.priceenddate,crr.priceregion,crr.pricecategory,crr.pricecustomercategory,crr.pricepurchasequantity,'C'as delta_mode\n" +
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
                    "            coalesce (crr.pricepurchasequantity, 0) <> coalesce (prev.pricepurchasequantity, 0))) order by rand() limit %s";

    public static String GET_PROD_PRICE_REC_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, product_type, pricecurrency, priceamount, pricestartdate, priceenddate, priceregion,pricecategory,pricecustomercategory,pricepurchasequantity\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select eprid, u_key, sourceref, modifiedon, metadeleted, product_type, pricecurrency, priceamount, pricestartdate, priceenddate, priceregion,pricecategory,pricecustomercategory,pricepurchasequantity\n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_product_prices_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",product_type as product_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pricecurrency as pricecurrency" +
                    ",priceamount as priceamount" +
                    ",pricestartdate as pricestartdate" +
                    ",priceenddate as priceenddate" +
                    ",priceregion as priceregion" +
                    ",pricecategory as pricecategory" +
                    ",pricecustomercategory as pricecustomercategory" +
                    ",pricepurchasequantity as pricepurchasequantity" +
                    ",delta_mode as DELTA_MODE" +
                    " from( \n" +
                    "    select crr.eprid,crr.u_key,crr.sourceref,crr.modifiedon,crr.metadeleted,crr.product_type,crr.pricecurrency,crr.priceamount,crr.pricestartdate,crr.priceenddate,crr.priceregion,crr.pricecategory,crr.pricecustomercategory,crr.pricepurchasequantity,'I'as delta_mode\n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select prev.eprid,prev.u_key,prev.sourceref,prev.modifiedon,prev.metadeleted,prev.product_type,prev.pricecurrency,prev.priceamount,prev.pricestartdate,prev.priceenddate,prev.priceregion,prev.pricecategory,prev.pricecustomercategory,prev.pricepurchasequantity,'D'as delta_mode\n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.eprid,crr.u_key,crr.sourceref,crr.modifiedon,crr.metadeleted,crr.product_type,crr.pricecurrency,crr.priceamount,crr.pricestartdate,crr.priceenddate,crr.priceregion,crr.pricecategory,crr.pricecustomercategory,crr.pricepurchasequantity,'C'as delta_mode\n" +
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
                    "            coalesce (crr.pricepurchasequantity, 0) <> coalesce (prev.pricepurchasequantity, 0))) where u_key in ('%s') order by u_key,eprid desc";


    public static String GET_RANDOM_KEY_PROD_PRICE_DELTA_CURRENT_HIST =
            "select u_key as UKEY" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_product_prices_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_product_prices_part) order by rand() limit %s";

    public static String GET_PROD_PRICE_DIFF_DELTA_CURRENT_HIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",product_type as product_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pricecurrency as pricecurrency" +
                    ",priceamount as priceamount" +
                    ",pricestartdate as pricestartdate" +
                    ",priceenddate as priceenddate" +
                    ",priceregion as priceregion" +
                    ",pricecategory as pricecategory" +
                    ",pricecustomercategory as pricecustomercategory" +
                    ",pricepurchasequantity as pricepurchasequantity" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_product_prices_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_product_prices_part) and u_key in ('%s') order by u_key,eprid desc";

    public static String GET_PROD_PRICE_REC_DELTA_CURRENT =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",product_type as product_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pricecurrency as pricecurrency" +
                    ",priceamount as priceamount" +
                    ",pricestartdate as pricestartdate" +
                    ",priceenddate as priceenddate" +
                    ",priceregion as priceregion" +
                    ",pricecategory as pricecategory" +
                    ",pricecustomercategory as pricecustomercategory" +
                    ",pricepurchasequantity as pricepurchasequantity" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_product_prices where u_key in ('%s') order by u_key,eprid desc";

    public static String GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select u_key, worksourceref, personsourceref, source, eprid, work_type, core_reference, roletype,rolename,title,person_first_name,person_family_name,email_address,honours,affiliation,imageurl,footnotetxt,notestxt,sequence,groupnumber,modifiedon,metadeleted \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select u_key, worksourceref, personsourceref, source, eprid, work_type, core_reference, roletype,rolename,title,person_first_name,person_family_name,email_address,honours,affiliation,imageurl,footnotetxt,notestxt,sequence,groupnumber,modifiedon,metadeleted \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    " select u_key as UKEY " +
                    " from( \n" +
                    "    select crr.u_key, crr.worksourceref, crr.personsourceref, crr.source, crr.eprid, crr.work_type, crr.core_reference, crr.roletype,crr.rolename,crr.title,crr.person_first_name,crr.person_family_name,crr.email_address,crr.honours,crr.affiliation,crr.imageurl,crr.footnotetxt,crr.notestxt,crr.sequence,crr.groupnumber,crr.modifiedon,crr.metadeleted,'I' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key,prev.worksourceref,prev.personsourceref, prev.source, prev.eprid, prev.work_type, prev.core_reference, prev.roletype,prev.rolename,prev.title,prev.person_first_name,prev.person_family_name,prev.email_address,prev.honours,prev.affiliation,prev.imageurl,prev.footnotetxt,prev.notestxt,prev.sequence,prev.groupnumber,prev.modifiedon,prev.metadeleted,'D' as delta_mode \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key, crr.worksourceref, crr.personsourceref, crr.source, crr.eprid, crr.work_type, crr.core_reference, crr.roletype,crr.rolename,crr.title,crr.person_first_name,crr.person_family_name,crr.email_address,crr.honours,crr.affiliation,crr.imageurl,crr.footnotetxt,crr.notestxt,crr.sequence,crr.groupnumber,crr.modifiedon,crr.metadeleted,'C' as delta_mode \n" +
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
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false)))order by rand() limit %s";

    public static String GET_PERSON_ROLE_REC_DIFF_TRANS_FILE =
            " with crr_dataset as(\n" +
                    "  select u_key, worksourceref, personsourceref, source, eprid, work_type, core_reference, roletype,rolename,title,person_first_name,person_family_name,email_address,honours,affiliation,imageurl,footnotetxt,notestxt,sequence,groupnumber,modifiedon,metadeleted \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part\n" +
                    "  where transform_file_ts = (select max(transform_file_ts ) \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part)\n" +
                    "  ),\n" +
                    "  prev_dataset as (\n" +
                    "  select u_key, worksourceref, personsourceref, source, eprid, work_type, core_reference, roletype,rolename,title,person_first_name,person_family_name,email_address,honours,affiliation,imageurl,footnotetxt,notestxt,sequence,groupnumber,modifiedon,metadeleted \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part\n" +
                    "  where transform_file_ts \n" +
                    "  = (select distinct transform_file_ts from \n" +
                    "  (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn  \n" +
                    "  from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_work_person_role_extended_transform_file_history_part dhap)where rn = 2))  \n" +
                    " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",worksourceref as worksourceref" +
                    ",personsourceref as personsourceref" +
                    ",source as source" +
                    ",work_type as work_type" +
                    ",core_reference as core_reference" +
                    ",roletype as roletype" +
                    ",rolename as rolename" +
                    ",title as title" +
                    ",person_first_name as person_first_name" +
                    ",person_family_name as person_family_name" +
                    ",email_address as email_address" +
                    ",honours as honours" +
                    ",affiliation as affiliation" +
                    ",imageurl as imageurl" +
                    ",footnotetxt as footnotetxt" +
                    ",notestxt as notestxt" +
                    ",sequence as sequence" +
                    ",groupnumber as groupnumber" +
                    //  ",metamodifiedon as metamodifiedon" +
                    ",metadeleted as metadeleted" +
                    ",delta_mode as DELTA_MODE" +
                    " from( \n" +
                    "    select crr.u_key, crr.worksourceref, crr.personsourceref, crr.source, crr.eprid, crr.work_type, crr.core_reference, crr.roletype,crr.rolename,crr.title,crr.person_first_name,crr.person_family_name,crr.email_address,crr.honours,crr.affiliation,crr.imageurl,crr.footnotetxt,crr.notestxt,crr.sequence,crr.groupnumber,crr.modifiedon,crr.metadeleted,'I' as delta_mode \n" +
                    "    from crr_dataset crr\n" +
                    "        left join prev_dataset prev on crr.u_key = prev.u_key\n" +
                    "    where prev.u_key is null\n" +
                    "    union\n" +
                    "    select  prev.u_key,prev.worksourceref,prev.personsourceref, prev.source, prev.eprid, prev.work_type, prev.core_reference, prev.roletype,prev.rolename,prev.title,prev.person_first_name,prev.person_family_name,prev.email_address,prev.honours,prev.affiliation,prev.imageurl,prev.footnotetxt,prev.notestxt,prev.sequence,prev.groupnumber,prev.modifiedon,prev.metadeleted,'D' as delta_mode \n" +
                    "    from prev_dataset prev\n" +
                    "        left join crr_dataset crr on crr.u_key = prev.u_key\n" +
                    "    where crr.u_key is null\n" +
                    "    union\n" +
                    "    select crr.u_key, crr.worksourceref, crr.personsourceref, crr.source, crr.eprid, crr.work_type, crr.core_reference, crr.roletype,crr.rolename,crr.title,crr.person_first_name,crr.person_family_name,crr.email_address,crr.honours,crr.affiliation,crr.imageurl,crr.footnotetxt,crr.notestxt,crr.sequence,crr.groupnumber,crr.modifiedon,crr.metadeleted,'C' as delta_mode \n" +
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
                    "            coalesce (crr.metadeleted, false) <> coalesce (prev.metadeleted, false))) where u_key in ('%s') order by u_key,eprid desc";


    public static String GET_RANDOM_KEY_WORK_PERS_ROLE_DELTA_CURRENT_HIST =
            "select u_key as UKEY" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_person_role_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_person_role_part) order by rand() limit %s";

    public static String GET_WORK_PERS_ROLE_REC_DIFF_DELTA_CURRENT_HIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY "+
                    ",worksourceref as worksourceref" +
                    ",personsourceref as personsourceref" +
                    ",source as source" +
                    ",work_type as work_type" +
                    ",core_reference as core_reference" +
                    ",roletype as roletype" +
                    ",rolename as rolename" +
                    ",title as title" +
                    ",person_first_name as person_first_name" +
                    ",person_family_name as person_family_name" +
                    ",email_address as email_address" +
                    ",honours as honours" +
                    ",affiliation as affiliation" +
                    ",imageurl as imageurl" +
                    ",footnotetxt as footnotetxt" +
                    ",notestxt as notestxt" +
                    ",sequence as sequence" +
                    ",groupnumber as groupnumber" +
                    //  ",metamodifiedon as metamodifiedon" +
                    ",metadeleted as metadeleted" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_person_role_part where " +
                    "delta_ts = (select max(delta_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_history_extended_work_person_role_part) and u_key in ('%s') order by u_key,eprid desc";

    public static String GET_WORK_PERS_ROLE_REC_DELTA_CURR =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",worksourceref as worksourceref" +
                    ",personsourceref as personsourceref" +
                    ",source as source" +
                    ",work_type as work_type" +
                    ",core_reference as core_reference" +
                    ",roletype as roletype" +
                    ",rolename as rolename" +
                    ",title as title" +
                    ",person_first_name as person_first_name" +
                    ",person_family_name as person_family_name" +
                    ",email_address as email_address" +
                    ",honours as honours" +
                    ",affiliation as affiliation" +
                    ",imageurl as imageurl" +
                    ",footnotetxt as footnotetxt" +
                    ",notestxt as notestxt" +
                    ",sequence as sequence" +
                    ",groupnumber as groupnumber" +
                    //  ",metamodifiedon as metamodifiedon" +
                    ",metadeleted as metadeleted" +
                    ",delta_mode as DELTA_MODE" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_person_role where u_key in ('%s') order by u_key,eprid desc";


    public static String GET_RANDOM_AVAILABILITY_KEY_DIFF_DELTACURR_CURRHIST =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_availability d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_part c )) order by rand() limit %s";

    public static String GET_AVAILABILITY_REC_DIFF_DELTACURR_CURRHIST =
            " select u_key as UKEY " +
                    ",sourceref as SOURCEREF" +
                    ",eprid as EPRID" +
                    ",producttype as PRODUCTTYPE" +
                    ",modifiedon as MODIFIEDON" +
                    ",application as APPLICATION" +
                    ",deltaanswercodeuk as deltaanswercodeuk" +
                    ",deltaanswercodeus as deltaanswercodeus" +
                    ",anzpubstatus as anzpubstatus" +
                    ",pubdateactual as pubdateactual" +
                    ",status as status" +
                    ",metadeleted as metadeleted from " +
                    "(select c.eprid,c.u_key,c.sourceref,c.modifiedon,c.application,c.deltaanswercodeuk,c.deltaanswercodeus," +
                    "c.anzpubstatus,c.pubdateactual,c.status,c.metadeleted,c.producttype " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_availability d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_part c )) where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_MANIF_EXT_KEY_DIFF_DELTACURR_CURRHIST =
            "select eprid as EPRID from \n" +
                    "(select c.eprid " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_part c )) order by rand() limit %s";

    public static String GET_MANIF_EXT_REC_DIFF_DELTACURR_CURRHIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",uktextbookind as uktextbookind" +
                    ",ustextbookind as ustextbookind" +
                    ",usdiscountcode as usdiscountcode" +
                    ",usdiscountname as usdiscountname" +
                    ",emeadiscountcode as emeadiscountcode" +
                    ",emeadiscountname as emeadiscountname" +
                    ",trimsize as trimsize" +
                    ",weight as weight" +
                    ",commcode as commcode" +
                    ",journalprodsitecode as journalprodsitecode" +
                    ",journalissuetrimsize as journalissuetrimsize" +
                    ",warreference as warreference from " +
                    "(select c.eprid, c.u_key, c.sourceref, c.modifiedon, c.metadeleted, c.manifestation_type, c.uktextbookind, c.ustextbookind,c.usdiscountcode,c.usdiscountname,c.emeadiscountcode,c.emeadiscountname,c.trimsize,c.weight,c.commcode,c.journalprodsitecode,c.journalissuetrimsize,c.warreference " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_part c )) where eprid in ('%s') order by eprid,u_key desc";


    public static String GET_RANDOM_PAGE_COUNT_KEY_DIFF_DELTACURR_CURRHIST =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_page_count d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_part c ))order by rand() limit %s";

    public static String GET_PAGE_COUNT_REC_DIFF_DELTACURR_CURRHIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pagecounttypecode as pagecounttypecode" +
                    ",pagecounttypename as pagecounttypename" +
                    ",pagecount as pagecount" +
                    " from (select c.eprid, c.u_key, c.sourceref, c.modifiedon, c.metadeleted, c.manifestation_type, c.pagecounttypecode, c.pagecounttypename,c.pagecount " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_page_count d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_part c )) where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_URL_KEY_DIFF_DELTACURR_CURRHIST =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_url d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_part c ))order by rand() limit %s";

    public static String GET_URL_DIFF_DELTACURR_CURRHIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",urltypecode as urltypecode" +
                    ",urltypecode as urltypecode" +
                    ",source as source" +
                    ",name as name from " +
                    "(select c.eprid, c.u_key, c.sourceref, c.modifiedon, c.metadeleted, c.work_type, c.urltypecode, c.urltypename, c.source, c.name " +
                    "from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_url d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_part c ))where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_WORK_EXT_KEY_DIFF_DELTACURR_CURRHIST =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_part c ))order by rand() limit %s";

    public static String GET_WORK_EXT_REC_DIFF_DELTACURR_CURRHIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",companygroup as companygroup" +
                    ",imagefileref as imagefileref" +
                    ",workmasterisbn as workmasterisbn" +
                    ",textreftrade as textreftrade" +
                    ",features as features" +
                    ",awards as awards" +
                    ",toc_long as toc_long" +
                    ",toc_short as toc_short" +
                    ",audience as audience" +
                    ",authorbyline as authorbyline" +
                    ",description as description" +
                    ",sbu as sbu" +
                    ",profitcentre as profitcentre" +
                    ",review as review" +
                    ",journalelscomind as journalelscomind" +
                    ",journalaimsscope as journalaimsscope" +
                    ",ddpeligibind as ddpeligibind" +
                    ",ptsjournalind as ptsjournalind" +
                    ",authorfeedbackind as authorfeedbackind" +
                    ",deltabusinessunit as deltabusinessunit" +
                    ",printername as printername" +
                    ",primarysitesystem as primarysitesystem" +
                    ",primarysiteacronym as primarysiteacronym" +
                    ",primarysitesupportlevel as primarysitesupportlevel" +
                    ",fulfilmentsystem as fulfilmentsystem" +
                    ",fulfilmentjournalacronym as fulfilmentjournalacronym" +
                    ",issueprodtypecode as issueprodtypecode" +
                    ",cataloguevolumesqty as cataloguevolumesqty" +
                    ",catalogueissuesqty as catalogueissuesqty" +
                    ",cataloguevolumefrom as cataloguevolumefrom" +
                    ",cataloguevolumeto as cataloguevolumeto" +
                    ",rfissuesqty as rfissuesqty" +
                    ",rftotalpagesqty as rftotalpagesqty" +
                    ",rffvi as rffvi" +
                    ",rflvi as rflvi" +
                    ",journalprevioustitle as journalprevioustitle" +
                    ",journalprimaryauthor as journalprimaryauthor from " +
                    "(select c.eprid, c.u_key, c.sourceref, c.modifiedon, c.metadeleted, c.work_type, c.companygroup, c.imagefileref, c.workmasterisbn, c.textreftrade, c.features,c.awards,c.toc_long,c.toc_short,c.audience,c.authorbyline,c.description,c.sbu,c.profitcentre,c.review,c.journalelscomind,c.journalaimsscope,c.ddpeligibind,c.ptsjournalind,c.authorfeedbackind,c.deltabusinessunit,c.printername" +
                    ",c.primarysitesystem,c.primarysiteacronym,c.primarysitesupportlevel,c.fulfilmentsystem,c.fulfilmentjournalacronym,c.issueprodtypecode," +
                    "c.cataloguevolumesqty,c.catalogueissuesqty,c.cataloguevolumefrom,c.cataloguevolumeto,c.rfissuesqty," +
                    "c.rftotalpagesqty,c.rffvi,c.rflvi,c.journalprevioustitle,c.journalprimaryauthor" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_part c ))where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_WORK_SUB_AREA_KEY_DIFF_DELTACURR_CURRHIST =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_subject_area d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_part c ))order by rand() limit %s";

    public static String GET_WORK_SUBJ_AREA_REC_DIFF_DELTACURR_CURRHIST =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",typecode as typecode" +
                    ",typedesc as typedesc" +
                    ",subjcode as subjcode" +
                    ",subjdesc as subjdesc" +
                    ",priority as priority from " +
                    "(select c.eprid, c.u_key, c.sourceref, c.modifiedon, c.metadeleted, c.work_type, c.typecode, c.typedesc, c.subjcode, c.subjdesc, c.priority " +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_subject_area d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_part c ))where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_MANIF_RESTRICT_KEY_DIFF_DELTACURR_CURRHIST =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation_restrictions d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_part c ))order by rand() limit %s";

    public static String GET_MANIF_RESTRICT_REC_DIFF_DELTACURR_CURRHIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",restrictioncode as restrictioncode" +
                    ",restrictionname as restrictionname from " +
                    "(select c.eprid, c.u_key, c.sourceref, c.modifiedon, c.metadeleted, c.manifestation_type, c.restrictioncode, c.restrictionname" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation_restrictions d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_part c ))where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_PROD_PRICE_KEY_DIFF_DELTACURR_CURRHIST =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_product_prices d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_part c ))order by rand() limit %s";

    public static String GET_PROD_PRICE_REC_DIFF_DELTACURR_CURRHIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",product_type as product_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pricecurrency as pricecurrency" +
                    ",priceamount as priceamount" +
                    ",pricestartdate as pricestartdate" +
                    ",priceenddate as priceenddate" +
                    ",priceregion as priceregion" +
                    ",pricecategory as pricecategory" +
                    ",pricecustomercategory as pricecustomercategory" +
                    ",pricepurchasequantity as pricepurchasequantity from " +
                    "(select c.eprid,c.u_key,c.sourceref,c.modifiedon,c.metadeleted,c.product_type,c.pricecurrency,c.priceamount,c.pricestartdate,c.priceenddate,c.priceregion,c.pricecategory,c.pricecustomercategory,c.pricepurchasequantity" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_product_prices d on c.eprid  = d.eprid \n" +
                    "where d.eprid is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_part c ))where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_WORK_PERS_ROLE_KEY_DIFF_DELTACURR_CURRHIST =
            "select u_key as EPRID from \n" +
                    "(select c.u_key from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_person_role d on c.u_key  = d.u_key \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_part c ))order by rand() limit %s";


    public static String GET_WORK_PERS_ROLE_REC_DIFF_DELTACURR_CURRHIST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",worksourceref as worksourceref" +
                    ",personsourceref as personsourceref" +
                    ",source as source" +
                    ",work_type as work_type" +
                    ",core_reference as core_reference" +
                    ",roletype as roletype" +
                    ",rolename as rolename" +
                    ",title as title" +
                    ",person_first_name as person_first_name" +
                    ",person_family_name as person_family_name" +
                    ",email_address as email_address" +
                    ",honours as honours" +
                    ",affiliation as affiliation" +
                    ",imageurl as imageurl" +
                    ",footnotetxt as footnotetxt" +
                    ",notestxt as notestxt" +
                    ",sequence as sequence" +
                    ",groupnumber as groupnumber" +
                    //  ",metamodifiedon as metamodifiedon" +
                    ",metadeleted as metadeleted from " +
                    "(select c.u_key, c.worksourceref, c.personsourceref, c.source, c.eprid, c.work_type, c.core_reference, c.roletype,c.rolename,c.title,c.person_first_name," +
                    "c.person_family_name,c.email_address,c.honours,c.affiliation,c.imageurl,c.footnotetxt,c.notestxt," +
                    "c.sequence,c.groupnumber,c.modifiedon,c.metadeleted" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_part c\n" +
                    "left join "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_person_role d on c.eprid  = d.eprid \n" +
                    "where d.u_key is null and c.transform_ts = (\n" +
                    "select max(c.transform_ts) from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_part c ))where u_key in ('%s') order by u_key desc";


    public static String GET_AVAILABILITY_REC_EXCL_DELTA =
            " select u_key as UKEY " +
                    ",sourceref as SOURCEREF" +
                    ",eprid as EPRID" +
                    ",producttype as PRODUCTTYPE" +
                    ",modifiedon as MODIFIEDON" +
                    ",application as APPLICATION" +
                    ",deltaanswercodeuk as deltaanswercodeuk" +
                    ",deltaanswercodeus as deltaanswercodeus" +
                    ",anzpubstatus as anzpubstatus" +
                    ",pubdateactual as pubdateactual" +
                    ",status as status" +
                    ",metadeleted as metadeleted" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_excl_delta where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_MANIF_EXT_REC_EXCL_DELTA =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",uktextbookind as uktextbookind" +
                    ",ustextbookind as ustextbookind" +
                    ",usdiscountcode as usdiscountcode" +
                    ",usdiscountname as usdiscountname" +
                    ",emeadiscountcode as emeadiscountcode" +
                    ",emeadiscountname as emeadiscountname" +
                    ",trimsize as trimsize" +
                    ",weight as weight" +
                    ",commcode as commcode" +
                    ",journalprodsitecode as journalprodsitecode" +
                    ",journalissuetrimsize as journalissuetrimsize" +
                    ",warreference as warreference" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_excl_delta where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_PAGE_COUNT_REC_EXCL_DELTA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pagecounttypecode as pagecounttypecode" +
                    ",pagecounttypename as pagecounttypename" +
                    ",pagecount as pagecount" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_excl_delta where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_URL_REC_EXCL_DELTA =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",urltypecode as urltypecode" +
                    ",urltypecode as urltypecode" +
                    ",source as source" +
                    ",name as name" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_excl_delta where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_WORK_EXT_REC_EXCL_DELTA =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",companygroup as companygroup" +
                    ",imagefileref as imagefileref" +
                    ",workmasterisbn as workmasterisbn" +
                    ",textreftrade as textreftrade" +
                    ",features as features" +
                    ",awards as awards" +
                    ",toc_long as toc_long" +
                    ",toc_short as toc_short" +
                    ",audience as audience" +
                    ",authorbyline as authorbyline" +
                    ",description as description" +
                    ",sbu as sbu" +
                    ",profitcentre as profitcentre" +
                    ",review as review" +
                    ",journalelscomind as journalelscomind" +
                    ",journalaimsscope as journalaimsscope" +
                    ",ddpeligibind as ddpeligibind" +
                    ",ptsjournalind as ptsjournalind" +
                    ",authorfeedbackind as authorfeedbackind" +
                    ",deltabusinessunit as deltabusinessunit" +
                    ",printername as printername" +
                    ",primarysitesystem as primarysitesystem" +
                    ",primarysiteacronym as primarysiteacronym" +
                    ",primarysitesupportlevel as primarysitesupportlevel" +
                    ",fulfilmentsystem as fulfilmentsystem" +
                    ",fulfilmentjournalacronym as fulfilmentjournalacronym" +
                    ",issueprodtypecode as issueprodtypecode" +
                    ",cataloguevolumesqty as cataloguevolumesqty" +
                    ",catalogueissuesqty as catalogueissuesqty" +
                    ",cataloguevolumefrom as cataloguevolumefrom" +
                    ",cataloguevolumeto as cataloguevolumeto" +
                    ",rfissuesqty as rfissuesqty" +
                    ",rftotalpagesqty as rftotalpagesqty" +
                    ",rffvi as rffvi" +
                    ",rflvi as rflvi" +
                    ",journalprevioustitle as journalprevioustitle" +
                    ",journalprimaryauthor as journalprimaryauthor" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_excl_delta where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_WORK_SUB_AREA_REC_EXCL_DELTA =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",typecode as typecode" +
                    ",typedesc as typedesc" +
                    ",subjcode as subjcode" +
                    ",subjdesc as subjdesc" +
                    ",priority as priority" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_excl_delta where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_MANIF_RESTRICT_REC_DIFF_EXCL_DELTA =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",restrictioncode as restrictioncode" +
                    ",restrictionname as restrictionname" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_excl_delta where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_PROD_PRICE_EXT_REC_EXCL_DELTA =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",product_type as product_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pricecurrency as pricecurrency" +
                    ",priceamount as priceamount" +
                    ",pricestartdate as pricestartdate" +
                    ",priceenddate as priceenddate" +
                    ",priceregion as priceregion" +
                    ",pricecategory as pricecategory" +
                    ",pricecustomercategory as pricecustomercategory" +
                    ",pricepurchasequantity as pricepurchasequantity" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_excl_delta where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_WORK_PERS_ROLE_REC_EXCL_DELTA =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",worksourceref as worksourceref" +
                    ",personsourceref as personsourceref" +
                    ",source as source" +
                    ",work_type as work_type" +
                    ",core_reference as core_reference" +
                    ",roletype as roletype" +
                    ",rolename as rolename" +
                    ",title as title" +
                    ",person_first_name as person_first_name" +
                    ",person_family_name as person_family_name" +
                    ",email_address as email_address" +
                    ",honours as honours" +
                    ",affiliation as affiliation" +
                    ",imageurl as imageurl" +
                    ",footnotetxt as footnotetxt" +
                    ",notestxt as notestxt" +
                    ",sequence as sequence" +
                    ",groupnumber as groupnumber" +
                    ",metadeleted as metadeleted" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_excl_delta where u_key in ('%s') order by u_key desc";

    public static String GET_RANDOM_AVAILABILITY_KEY_SUM_DELTACURR_EXCL =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_excl_delta as c union all \n" +
                    "select d.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_availability as d) order by rand() limit %s";

    public static String GET_AVAILABILITY_REC_SUM_DELTACURR_EXCL =
            " select u_key as UKEY " +
                    ",sourceref as SOURCEREF" +
                    ",eprid as EPRID" +
                    ",producttype as PRODUCTTYPE" +
                    ",modifiedon as MODIFIEDON" +
                    ",application as APPLICATION" +
                    ",deltaanswercodeuk as deltaanswercodeuk" +
                    ",deltaanswercodeus as deltaanswercodeus" +
                    ",anzpubstatus as anzpubstatus" +
                    ",pubdateactual as pubdateactual" +
                    ",status as status" +
                    ",metadeleted as metadeleted from " +
                    "(select c.eprid,c.u_key,c.sourceref,c.modifiedon,c.application,c.deltaanswercodeuk,c.deltaanswercodeus,c.anzpubstatus,c.pubdateactual,c.status,c.metadeleted,c.producttype" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_excl_delta as c union all \n" +
                    "select d.eprid,d.u_key,d.sourceref,d.modifiedon,d.application,d.deltaanswercodeuk,d.deltaanswercodeus,d.anzpubstatus,d.pubdateactual,d.status,d.metadeleted,d.producttype" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_availability as d) where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_MANIF_EXT_KEY_SUM_DELTACURR_EXCL =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_excl_delta as c union all \n" +
                    "select d.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation as d)order by rand() limit %s";

    public static String GET_MANIF_EXT_REC_SUM_DELTACURR_EXCL =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",uktextbookind as uktextbookind" +
                    ",ustextbookind as ustextbookind" +
                    ",usdiscountcode as usdiscountcode" +
                    ",usdiscountname as usdiscountname" +
                    ",emeadiscountcode as emeadiscountcode" +
                    ",emeadiscountname as emeadiscountname" +
                    ",trimsize as trimsize" +
                    ",weight as weight" +
                    ",commcode as commcode" +
                    ",journalprodsitecode as journalprodsitecode" +
                    ",journalissuetrimsize as journalissuetrimsize" +
                    ",warreference as warreference" +
                    " from \n" +
                    "(select c.eprid, c.u_key, c.sourceref, c.modifiedon, c.metadeleted, c.manifestation_type, c.uktextbookind, c.ustextbookind,c.usdiscountcode,c.usdiscountname,c.emeadiscountcode,c.emeadiscountname,c.trimsize,c.weight,c.commcode,c.journalprodsitecode,c.journalissuetrimsize,c.warreference" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_excl_delta as c union all \n" +
                    "select d.eprid, d.u_key, d.sourceref, d.modifiedon, d.metadeleted, d.manifestation_type, d.uktextbookind, d.ustextbookind,d.usdiscountcode,d.usdiscountname,d.emeadiscountcode,d.emeadiscountname,d.trimsize,d.weight,d.commcode,d.journalprodsitecode,d.journalissuetrimsize,d.warreference" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation as d)where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_PAGE_COUNT_KEY_SUM_DELTACURR_EXCL =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_excl_delta as c union all \n" +
                    "select d.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_page_count as d)order by rand() limit %s";

    public static String GET_PAGE_COUNT_REC_SUM_DELTACURR_EXCL =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pagecounttypecode as pagecounttypecode" +
                    ",pagecounttypename as pagecounttypename" +
                    ",pagecount as pagecount" +
                    " from \n" +
                    "(select c.eprid, c.u_key, c.sourceref, c.modifiedon, c.metadeleted, c.manifestation_type, c.pagecounttypecode, c.pagecounttypename,c.pagecount" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_excl_delta as c union all \n" +
                    "select d.eprid, d.u_key, d.sourceref, d.modifiedon, d.metadeleted, d.manifestation_type, d.pagecounttypecode, d.pagecounttypename,d.pagecount" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_page_count as d)where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_URL_KEY_SUM_DELTACURR_EXCL =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_excl_delta as c union all \n" +
                    "select d.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_url as d)order by rand() limit %s";

    public static String GET_URL_SUM_REC_DELTACURR_EXCL =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",urltypecode as urltypecode" +
                    ",urltypecode as urltypecode" +
                    ",source as source" +
                    ",name as name" +
                    " from \n" +
                    "(select c.eprid, c.u_key, c.sourceref, c.modifiedon, c.metadeleted, c.work_type, c.urltypecode, c.urltypename, c.source, c.name " +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_excl_delta as c union all \n" +
                    "select d.eprid, d.u_key, d.sourceref, d.modifiedon, d.metadeleted, d.work_type, d.urltypecode, d.urltypename, d.source, d.name " +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_url as d)" +
                    "where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_WORK_EXT_KEY_SUM_DELTACURR_EXCL =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_excl_delta as c union all \n" +
                    "select d.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work as d)order by rand() limit %s";

    public static String GET_WORK_EXT_REC_SUM_DELTACURR_EXCL =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",companygroup as companygroup" +
                    ",imagefileref as imagefileref" +
                    ",workmasterisbn as workmasterisbn" +
                    ",textreftrade as textreftrade" +
                    ",features as features" +
                    ",awards as awards" +
                    ",toc_long as toc_long" +
                    ",toc_short as toc_short" +
                    ",audience as audience" +
                    ",authorbyline as authorbyline" +
                    ",description as description" +
                    ",sbu as sbu" +
                    ",profitcentre as profitcentre" +
                    ",review as review" +
                    ",journalelscomind as journalelscomind" +
                    ",journalaimsscope as journalaimsscope" +
                    ",ddpeligibind as ddpeligibind" +
                    ",ptsjournalind as ptsjournalind" +
                    ",authorfeedbackind as authorfeedbackind" +
                    ",deltabusinessunit as deltabusinessunit" +
                    ",printername as printername" +
                    ",primarysitesystem as primarysitesystem" +
                    ",primarysiteacronym as primarysiteacronym" +
                    ",primarysitesupportlevel as primarysitesupportlevel" +
                    ",fulfilmentsystem as fulfilmentsystem" +
                    ",fulfilmentjournalacronym as fulfilmentjournalacronym" +
                    ",issueprodtypecode as issueprodtypecode" +
                    ",cataloguevolumesqty as cataloguevolumesqty" +
                    ",catalogueissuesqty as catalogueissuesqty" +
                    ",cataloguevolumefrom as cataloguevolumefrom" +
                    ",cataloguevolumeto as cataloguevolumeto" +
                    ",rfissuesqty as rfissuesqty" +
                    ",rftotalpagesqty as rftotalpagesqty" +
                    ",rffvi as rffvi" +
                    ",rflvi as rflvi" +
                    ",journalprevioustitle as journalprevioustitle" +
                    ",journalprimaryauthor as journalprimaryauthor" +
                    " from \n" +
                    "(select c.eprid, c.u_key, c.sourceref, c.modifiedon, c.metadeleted, c.work_type, c.companygroup, c.imagefileref, c.workmasterisbn, c.textreftrade, c.features,c.awards,c.toc_long,c.toc_short,c.audience,c.authorbyline,c.description,c.sbu,c.profitcentre,c.review,c.journalelscomind,c.journalaimsscope,c.ddpeligibind,c.ptsjournalind,c.authorfeedbackind,c.deltabusinessunit,c.printername" +
                    ",c.primarysitesystem,c.primarysiteacronym,c.primarysitesupportlevel,c.fulfilmentsystem,c.fulfilmentjournalacronym,c.issueprodtypecode," +
                    "c.cataloguevolumesqty,c.catalogueissuesqty,c.cataloguevolumefrom,c.cataloguevolumeto,c.rfissuesqty," +
                    "c.rftotalpagesqty,c.rffvi,c.rflvi,c.journalprevioustitle,c.journalprimaryauthor" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_excl_delta as c union all \n" +
                    "select d.eprid, d.u_key, d.sourceref, d.modifiedon, d.metadeleted, d.work_type, d.companygroup, d.imagefileref, d.workmasterisbn, d.textreftrade, d.features,d.awards,d.toc_long,d.toc_short,d.audience,d.authorbyline,d.description,d.sbu,d.profitcentre,d.review,d.journalelscomind,d.journalaimsscope,d.ddpeligibind,d.ptsjournalind,d.authorfeedbackind,d.deltabusinessunit,d.printername" +
                    ",d.primarysitesystem,d.primarysiteacronym,d.primarysitesupportlevel,d.fulfilmentsystem,d.fulfilmentjournalacronym,d.issueprodtypecode," +
                    "d.cataloguevolumesqty,d.catalogueissuesqty,d.cataloguevolumefrom,d.cataloguevolumeto,d.rfissuesqty," +
                    "d.rftotalpagesqty,d.rffvi,d.rflvi,d.journalprevioustitle,d.journalprimaryauthor" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work as d) where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_WORK_SUB_AREA_KEY_SUM_DELTACURR_EXCL =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_excl_delta as c union all \n" +
                    "select d.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_subject_area as d)order by rand() limit %s";

    public static String GET_WORK_SUB_AREA_REC_SUM_DELTACURR_EXCL =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",typecode as typecode" +
                    ",typedesc as typedesc" +
                    ",subjcode as subjcode" +
                    ",subjdesc as subjdesc" +
                    ",priority as priority" +
                    " from \n" +
                    "(select c.eprid, c.u_key, c.sourceref, c.modifiedon, c.metadeleted, c.work_type, c.typecode, c.typedesc, c.subjcode, c.subjdesc, c.priority" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_excl_delta as c union all \n" +
                    "select d.eprid, d.u_key, d.sourceref, d.modifiedon, d.metadeleted, d.work_type, d.typecode, d.typedesc, d.subjcode, d.subjdesc, d.priority" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_subject_area as d)where eprid in ('%s') order by eprid,u_key desc";


    public static String GET_RANDOM_MANIF_RESTRICT_KEY_SUM_DELTACURR_EXCL =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_excl_delta as c union all \n" +
                    "select d.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation_restrictions as d)order by rand() limit %s";

    public static String GET_MANIF_RESTRICT_REC_SUM_DELTACURR_EXCL =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",restrictioncode as restrictioncode" +
                    ",restrictionname as restrictionname" +
                    " from \n" +
                    "(select c.eprid, c.u_key, c.sourceref, c.modifiedon, c.metadeleted, c.manifestation_type, c.restrictioncode, c.restrictionname" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_excl_delta as c union all \n" +
                    " select d.eprid, d.u_key, d.sourceref, d.modifiedon, d.metadeleted, d.manifestation_type, d.restrictioncode, d.restrictionname" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_manifestation_restrictions as d)where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_PROD_PRICE_KEY_SUM_DELTACURR_EXCL =
            "select eprid as EPRID from \n" +
                    "(select c.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_excl_delta as c union all \n" +
                    "select d.eprid from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_product_prices as d)order by rand() limit %s";

    public static String GET_PROD_PRICE_REC_SUM_DELTACURR_EXCL =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",product_type as product_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pricecurrency as pricecurrency" +
                    ",priceamount as priceamount" +
                    ",pricestartdate as pricestartdate" +
                    ",priceenddate as priceenddate" +
                    ",priceregion as priceregion" +
                    ",pricecategory as pricecategory" +
                    ",pricecustomercategory as pricecustomercategory" +
                    ",pricepurchasequantity as pricepurchasequantity" +
                    " from \n" +
                    "(select c.eprid,c.u_key,c.sourceref,c.modifiedon,c.metadeleted,c.product_type,c.pricecurrency,c.priceamount,c.pricestartdate,c.priceenddate,c.priceregion,c.pricecategory,c.pricecustomercategory,c.pricepurchasequantity" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_excl_delta as c union all \n" +
                    "select d.eprid,d.u_key,d.sourceref,d.modifiedon,d.metadeleted,d.product_type,d.pricecurrency,d.priceamount,d.pricestartdate,d.priceenddate,d.priceregion,d.pricecategory,d.pricecustomercategory,d.pricepurchasequantity" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_product_prices as d)where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_RANDOM_WORK_PERS_ROLE_KEY_SUM_DELTACURR_EXCL =
            "select u_key as EPRID from \n" +
                    "(select c.u_key from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_excl_delta as c union all \n" +
                    "select d.u_key from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_person_role as d)order by rand() limit %s";

    public static String GET_WORK_PERSON_ROLE_REC_SUM_DELTACURR_EXCL =
            " select u_key as UKEY" +
                    ",worksourceref as worksourceref" +
                    ",personsourceref as personsourceref" +
                    ",source as source" +
                    ",work_type as work_type" +
                    ",core_reference as core_reference" +
                    ",roletype as roletype" +
                    ",rolename as rolename" +
                    ",title as title" +
                    ",person_first_name as person_first_name" +
                    ",person_family_name as person_family_name" +
                    ",email_address as email_address" +
                    ",honours as honours" +
                    ",affiliation as affiliation" +
                    ",imageurl as imageurl" +
                    ",footnotetxt as footnotetxt" +
                    ",notestxt as notestxt" +
                    ",sequence as sequence" +
                    ",groupnumber as groupnumber" +
                    //  ",metamodifiedon as metamodifiedon" +
                    ",metadeleted as metadeleted" +
                    " from \n" +
                    "(select c.u_key, c.worksourceref, c.personsourceref, c.source, c.work_type, c.core_reference, c.roletype,c.rolename,c.title,c.person_first_name,c.person_family_name,c.email_address,c.honours,c.affiliation,c.imageurl,c.footnotetxt,c.notestxt," +
                    "c.sequence,c.groupnumber,c.modifiedon,c.metadeleted" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_excl_delta as c union all \n" +
                    "select d.u_key, d.worksourceref, d.personsourceref, d.source, d.work_type, d.core_reference, d.roletype,d.rolename,d.title,d.person_first_name,d.person_family_name,d.email_address,d.honours,d.affiliation,d.imageurl,d.footnotetxt,d.notestxt," +
                    "d.sequence,d.groupnumber,d.modifiedon,d.metadeleted" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_delta_current_extended_work_person_role as d)where u_key in ('%s') order by u_key desc";

    public static String GET_AVAILABILITY_REC_LATEST =
            " select u_key as UKEY " +
                    ",sourceref as SOURCEREF" +
                    ",eprid as EPRID" +
                    ",producttype as PRODUCTTYPE" +
                    ",modifiedon as MODIFIEDON" +
                    ",application as APPLICATION" +
                    ",deltaanswercodeuk as deltaanswercodeuk" +
                    ",deltaanswercodeus as deltaanswercodeus" +
                    ",anzpubstatus as anzpubstatus" +
                    ",pubdateactual as pubdateactual" +
                    ",status as status" +
                    ",metadeleted as metadeleted" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_availability_latest where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_MANIF_EXT_REC_LATEST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",uktextbookind as uktextbookind" +
                    ",ustextbookind as ustextbookind" +
                    ",usdiscountcode as usdiscountcode" +
                    ",usdiscountname as usdiscountname" +
                    ",emeadiscountcode as emeadiscountcode" +
                    ",emeadiscountname as emeadiscountname" +
                    ",trimsize as trimsize" +
                    ",weight as weight" +
                    ",commcode as commcode" +
                    ",journalprodsitecode as journalprodsitecode" +
                    ",journalissuetrimsize as journalissuetrimsize" +
                    ",warreference as warreference" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_latest where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_PAGE_COUNT_IDENT_REC_LATEST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pagecounttypecode as pagecounttypecode" +
                    ",pagecounttypename as pagecounttypename" +
                    ",pagecount as pagecount" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_page_count_latest where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_URL_EXT_LATEST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",urltypecode as urltypecode" +
                    ",urltypecode as urltypecode" +
                    ",source as source" +
                    ",name as name" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_url_latest where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_WRK_EXT_LATEST_COUNT =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",companygroup as companygroup" +
                    ",imagefileref as imagefileref" +
                    ",workmasterisbn as workmasterisbn" +
                    ",textreftrade as textreftrade" +
                    ",features as features" +
                    ",awards as awards" +
                    ",toc_long as toc_long" +
                    ",toc_short as toc_short" +
                    ",audience as audience" +
                    ",authorbyline as authorbyline" +
                    ",description as description" +
                    ",sbu as sbu" +
                    ",profitcentre as profitcentre" +
                    ",review as review" +
                    ",journalelscomind as journalelscomind" +
                    ",journalaimsscope as journalaimsscope" +
                    ",ddpeligibind as ddpeligibind" +
                    ",ptsjournalind as ptsjournalind" +
                    ",authorfeedbackind as authorfeedbackind" +
                    ",deltabusinessunit as deltabusinessunit" +
                    ",printername as printername" +
                    ",primarysitesystem as primarysitesystem" +
                    ",primarysiteacronym as primarysiteacronym" +
                    ",primarysitesupportlevel as primarysitesupportlevel" +
                    ",fulfilmentsystem as fulfilmentsystem" +
                    ",fulfilmentjournalacronym as fulfilmentjournalacronym" +
                    ",issueprodtypecode as issueprodtypecode" +
                    ",cataloguevolumesqty as cataloguevolumesqty" +
                    ",catalogueissuesqty as catalogueissuesqty" +
                    ",cataloguevolumefrom as cataloguevolumefrom" +
                    ",cataloguevolumeto as cataloguevolumeto" +
                    ",rfissuesqty as rfissuesqty" +
                    ",rftotalpagesqty as rftotalpagesqty" +
                    ",rffvi as rffvi" +
                    ",rflvi as rflvi" +
                    ",journalprevioustitle as journalprevioustitle" +
                    ",journalprimaryauthor as journalprimaryauthor" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_latest where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_WORK_SUB_AREA_REC_LATEST =
            "select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",work_type as work_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",typecode as typecode" +
                    ",typedesc as typedesc" +
                    ",subjcode as subjcode" +
                    ",subjdesc as subjdesc" +
                    ",priority as priority" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_subject_area_latest where eprid in ('%s') order by eprid desc";

    public static String GET_MANIF_RESTRICT_REC_LATEST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",manifestation_type as manifestation_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",restrictioncode as restrictioncode" +
                    ",restrictionname as restrictionname" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_manifestation_restrictions_latest where eprid in ('%s') order by eprid,u_key desc";

    public static String GET_WORK_PERSON_ROLE_REC_LATEST =
            " select u_key as UKEY " +
                    ",worksourceref as worksourceref" +
                    ",personsourceref as personsourceref" +
                    ",source as source" +
                    ",work_type as work_type" +
                    ",core_reference as core_reference" +
                    ",roletype as roletype" +
                    ",rolename as rolename" +
                    ",title as title" +
                    ",person_first_name as person_first_name" +
                    ",person_family_name as person_family_name" +
                    ",email_address as email_address" +
                    ",honours as honours" +
                    ",affiliation as affiliation" +
                    ",imageurl as imageurl" +
                    ",footnotetxt as footnotetxt" +
                    ",notestxt as notestxt" +
                    ",sequence as sequence" +
                    ",groupnumber as groupnumber" +
                    ",metadeleted as metadeleted" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_work_person_role_latest where u_key in ('%s') order by u_key desc";

    public static String GET_PROD_PRICE_REC_LATEST =
            " select eprid as EPRID " +
                    ",u_key as UKEY" +
                    ",product_type as product_type" +
                    ",sourceref as SOURCEREF" +
                    ",modifiedon as MODIFIEDON" +
                    ",metadeleted as metadeleted" +
                    ",pricecurrency as pricecurrency" +
                    ",priceamount as priceamount" +
                    ",pricestartdate as pricestartdate" +
                    ",priceenddate as priceenddate" +
                    ",priceregion as priceregion" +
                    ",pricecategory as pricecategory" +
                    ",pricecustomercategory as pricecustomercategory" +
                    ",pricepurchasequantity as pricepurchasequantity" +
                    " from "+GetBCS_ETLExtendedDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_extended_product_prices_latest where eprid in ('%s') order by eprid,u_key desc";


}



