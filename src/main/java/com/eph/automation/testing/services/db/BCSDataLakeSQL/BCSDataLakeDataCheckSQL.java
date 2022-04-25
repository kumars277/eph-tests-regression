//created by Nishant @ 29 Sep 2020
//updation on 19-04-2021 Dinesh removed hardcoded DB.

package com.eph.automation.testing.services.db.BCSDataLakeSQL;


public class BCSDataLakeDataCheckSQL {


    public static String randomId_ingestTableFor_stg_current_classification=
            "select sourceref from(SELECT cl.value, productprojectno sourceref\n" +
                    "FROM (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product f CROSS JOIN UNNEST(distributionclassification) x (cl)))\n" +
                    "where value is not null AND value!='' AND value!='0' order by rand() limit %s";

    public static String getInitialIngestDataFor_stg_current_classification=
            "select * from(SELECT\n" +
                    "  metainfdeleted metadeleted\n" +
                    ", metainfmodifiedon metamodifiedon\n" +
                    ", productprojectno sourceref\n" +
                    ", cl.classificationcode\n" +
                    ", cl.value value\n" +
                    ", cl.classificationtype\n" +
                    ", cl.priority\n" +
                    ", cl.businessunit\n" +
                    "FROM  (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product f\n" +
                    "CROSS JOIN UNNEST(distributionclassification) x (cl)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,value,classificationcode desc";

    public static String getCurrentTableDataFor_stg_current_classification=
            "select metadeleted, metamodifiedon,sourceref,classificationcode,\n" +
                    "value,classificationtype,priority,businessunit\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_classification \n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,value,classificationcode desc";


    //created by Nishant @ 21 Oct 2020
    public static String randomId_ingestTableFor_stg_current_content=
            "select sourceref from(SELECT df.productprojectno sourceref FROM\n" +
                    "bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df)  order by rand() limit %s";


    public static String getInitialIngestDataFor_stg_current_content=
            "select* from(SELECT\n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", df.contentoriginimpid originimpid\n" +
                    ", df.contentsubgroup subgroup\n" +
                    ", df.contentseries series\n" +
                    ", df.contentcopyrightyear copyrightyear\n" +
                    ", df.contenttitle title\n" +
                    ", df.contentworktitle worktitle\n" +
                    ", df.contentisset isset\n" +
                    "--, df.contentimpressionid impressionid\n" +
                    ", df.contentdoistatus doistatus\n" +
                    ", df.contentimprint imprint\n" +
                    ", df.contentdivision division\n" +
                    ", df.contentlanguage3 language3\n" +
                    ", df.contentregstatus regstatus\n" +
                    ", df.contentapprovedondate approvedondate\n" +
                    ", df.contentlanguage2 language2\n" +
                    ", df.contentdoi doi\n" +
                    ", df.contentvolumeno volumeno\n" +
                    ", df.contenteditionid editionid\n" +
                    ", df.contentsynctemplate synctemplate\n" +
                    ", df.contentvolumename volumename\n" +
                    ", df.contentseriesissn seriesissn\n" +
                    ", df.contentseriesid seriesid\n" +
                    ", df.contentownership ownership\n" +
                    ", df.contentfirstapproval firstapproval\n" +
                    ", df.contentcompanygroup companygroup\n" +
                    ", df.contentshorttitle shorttitle\n" +
                    ", df.contenteditionno editionno\n" +
                    ", df.contentwmyn work_master_flag\n" +
                    ", df.contentlanguage language\n" +
                    ", df.contentpiidack piidack\n" +
                    ", df.contentpublisher publisher\n" +
                    ", df.contenttitleid titleid\n" +
                    ", df.contentsubtitle subtitle\n" +
                    ", df.contentseriescode seriescode\n" +
                    ", df.contentobjtype objecttype\n" +
                    "FROM bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df) \n" +
                    "  where sourceref in ('%s') order by sourceref desc";

    public static String getCurrentTableDataFor_stg_current_content=
            "select metadeleted,metamodifiedon,sourceref,originimpid,subgroup,series,copyrightyear,title,worktitle," +
                    "isset,doistatus,imprint,division,language3,regstatus,approvedondate,language2,doi," +
                    "volumeno,editionid,synctemplate,volumename,seriesissn,seriesid,ownership,firstapproval,companygroup," +
                    "shorttitle,editionno,work_master_flag,language,piidack,publisher,titleid,subtitle,seriescode,objecttype " +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_content \n" +
                    "where sourceref in ('%s') order by sourceref desc";

    //added by Nishant @ 22 Oct 2020
    public static String randomId_ingestTableFor_stg_current_extobject=
            "select sourceref from(select productprojectno sourceref, cj.object, cj.name\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(productexternalobjects) x (cj)))\n" +
                    "where name is not null and name !='' and name!='0'\n" +
                    "order by rand() limit %s";

    public static String getInitialIngestDataFor_stg_current_extobject=
            "select * from(select" +
                    "  metainfdeleted metadeleted\n" +
                    ", metainfmodifiedon metamodifiedon\n" +
                    ", productprojectno sourceref\n" +
                    ", cj.object\n" +
                    ", cj.type\n" +
                    ", cj.name\n" +
                    ", cj.comments\n" +
                    ", cj.source\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(productexternalobjects) x (cj)))\n" +
                    "where sourceref in ('%s') \n" +
                    "order by sourceref,name,object desc";

    public static String getCurrentTableDataFor_stg_current_extobject=
            "select metadeleted,metamodifiedon,sourceref,object,type,name,comments,source \n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_extobject \n" +
                    "where sourceref in ('%s') order by sourceref,name,object desc";


    public static String randomId_ingestTableFor_stg_current_fullversionfamily=
            "select sourceref from (select productprojectno sourceref, fv.projectno\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(contentfullversionfamily) x (fv)))\n" +
                    "where projectno is not null and projectno!=''\n" +
                    "order by rand() limit %s";

    public static String getInitialIngestDataFor_stg_current_fullversionfamily=
            "select* from(select \n" +
                    "  metainfdeleted metadeleted\n" +
                    ", metainfmodifiedon metamodifiedon\n" +
                    ", productprojectno sourceref\n" +
                    ", fv.versiontype\n" +
                    ", fv.editionno\n" +
                    ", fv.isbn\n" +
                    ", fv.projectno\n" +
                    ", fv.workmaster\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(contentfullversionfamily) x (fv)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,projectno desc";

    public static String getCurrentTableDataFor_stg_current_fullversionfamily=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "versiontype,editionno,isbn,projectno,workmaster\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_fullversionfamily \n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,projectno desc";


    public static String randomId_ingestTableFor_stg_current_originatoraddress=
            "with originator as (select\n" +
                    "    df.metainfdeleted metadeleted\n" +
                    "  , df.metainfmodifiedon metamodifiedon\n" +
                    "  --, df.productprojectno sourceref\n" +
                    "  , co.businesspartnerid businesspartnerid\n" +
                    "  , co.authoraddress\n" +
                    "  from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "  CROSS JOIN UNNEST(contactsoriginators) x (co))),\n" +
                    "  address as (\n" +
                    "  select\n" +
                    "     uo.metadeleted\n" +
                    "   , uo.metamodifiedon\n" +
                    "--   , uo.sourceref\n" +
                    "   , uo.businesspartnerid\n" +
                    "   , ua.addressline2\n" +
                    "   , ua.country\n" +
                    "   , ua.fax\n" +
                    "   , ua.street\n" +
                    "   , ua.additionaladdress\n" +
                    "   , ua.city\n" +
                    "   , ua.addressline1\n" +
                    "   , ua.addressid\n" +
                    "   , ua.telephonemain\n" +
                    "   , ua.ismainaddress\n" +
                    "   , ua.internet\n" +
                    "   , ua.houseno\n" +
                    "   , ua.email\n" +
                    "   , ua.addressline3\n" +
                    "   , ua.telephoneother\n" +
                    "   , ua.postalcode\n" +
                    "   , ua.mobile\n" +
                    "   , ua.district\n" +
                    "   from (originator uo\n" +
                    "   CROSS JOIN UNNEST(authoraddress) z (ua)))\n" +
                    "select businesspartnerid from (select distinct\n" +
                    "* from address) where businesspartnerid is not null order by rand() limit %s";

    public static String getInitialIngestDataFor_stg_current_originatoraddress=
            "with originator as (\n" +
                    "  select\n" +
                    "    df.metainfdeleted metadeleted\n" +
                    "  , df.metainfmodifiedon metamodifiedon\n" +
                    "  --, df.productprojectno sourceref\n" +
                    "  , co.businesspartnerid businesspartnerid\n" +
                    "  , co.authoraddress\n" +
                    "  from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "  CROSS JOIN UNNEST(contactsoriginators) x (co))),\n" +
                    "  address as (\n" +
                    "  select\n" +
                    "     uo.metadeleted\n" +
                    "   , uo.metamodifiedon\n" +
                    "--   , uo.sourceref\n" +
                    "   , uo.businesspartnerid\n" +
                    "   , ua.addressline2\n" +
                    "   , ua.country\n" +
                    "   , ua.fax\n" +
                    "   , ua.street\n" +
                    "   , ua.additionaladdress\n" +
                    "   , ua.city\n" +
                    "   , ua.addressline1\n" +
                    "   , ua.addressid\n" +
                    "   , ua.telephonemain\n" +
                    "   , ua.ismainaddress\n" +
                    "   , ua.internet\n" +
                    "   , ua.houseno\n" +
                    "   , ua.email\n" +
                    "   , ua.addressline3\n" +
                    "   , ua.telephoneother\n" +
                    "   , ua.postalcode\n" +
                    "   , ua.mobile\n" +
                    "   , ua.district\n" +
                    "   from (originator uo\n" +
                    "   CROSS JOIN UNNEST(authoraddress) z (ua)))\n" +
                    "select * from (select distinct\n" +
                    "* from address) \n" +
                    "where businesspartnerid in (%s)\n" +
                    "order by businesspartnerid,country,metamodifiedon desc";

    public static String getCurrentTableDataFor_stg_current_originatoraddress=
            "select metadeleted,metamodifiedon,businesspartnerid,addressline2,\n" +
                    "country,fax,street,additionaladdress,city,addressline1,addressid,telephonemain,\n" +
                    "ismainaddress,internet,houseno,email,addressline3,telephoneother,postalcode,\n" +
                    "mobile,district from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originatoraddress \n" +
                    "where businesspartnerid in (%s)\n" +
                    "order by businesspartnerid,country,metamodifiedon desc";

    public static String randomId_ingestTableFor_stg_current_originators=
            "select sourceref from " +
                    "(select df.productprojectno sourceref, co.businesspartnerid businesspartnerid\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df CROSS JOIN UNNEST(contactsoriginators) x (co)))\n" +
                    "where businesspartnerid is not null order by rand() limit %s";


    public static String getInitialIngestDataFor_stg_current_originators=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", co.prefix prefix\n" +
                    ", co.sequence sequence\n" +
                    ", co.businesspartnerid businesspartnerid\n" +
                    ", co.originatortitle originatorid\n" +
                    ", co.isperson isperson\n" +
                    ", co.locationid locationid\n" +
                    ", co.copyrightholdertype copyrightholdertype\n" +
                    ", co.institution institution\n" +
                    ", co.firstname firstname\n" +
                    ", co.department department\n" +
                    ", co.lastname lastname\n" +
                    ", co.searchterm searchterm\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(contactsoriginators) x (co)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,copyrightholdertype,businesspartnerid desc";

    public static String getCurrentTableDataFor_stg_current_originators=
            "select metadeleted,metamodifiedon,sourceref, prefix,sequence,businesspartnerid,originatorid,\n" +
                    "isperson,locationid,copyrightholdertype,institution,\n" +
                    "firstname,department,lastname,searchterm\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originators \n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,copyrightholdertype,businesspartnerid desc";

    public static String randomId_ingestTableFor_stg_current_pricing=
            "select sourceref from(select \n" +
                    " df.productprojectno sourceref\n" +
                    ", cp.validupto validto\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(productprice) x (cp)))\n" +
                    "where validto is not null and validto!=''\n" +
                    "order by rand() limit %s";
    public static String getInitialIngestDataFor_stg_current_pricing=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", cp.validfrom validfrom\n" +
                    ", cp.type type\n" +
                    ", cp.currency currency\n" +
                    ", cp.priceapprox priceapprox\n" +
                    ", cp.price price\n" +
                    ", cp.validupto validto\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(productprice) x (cp)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,type,currency,validto desc";
    public static String getCurrentTableDataFor_stg_current_pricing=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "validfrom,type,currency,priceapprox,price,validto\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_pricing\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,type,currency,validto desc";

    public static String randomId_ingestTableFor_stg_current_product=
            "select sourceref from(select df.productprojectno sourceref\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df)) \n" +
                    "order by rand() limit %s";
    public static String getInitialIngestDataFor_stg_current_product=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", df.productnoofvolumes noofvolumes\n" +
                    ", df.productrefkey refkey\n" +
                    ", df.productexternaleditionid externaleditionid\n" +
                    ", df.productisbn13 isbn13\n" +
                    ", df.productversiontype versiontype\n" +
                    ", df.productpodsuitable podsuitable\n" +
                    ", df.productfirstrelease firstrelease\n" +
                    ", df.productorigtitle origtitle\n" +
                    ", df.productinventoryukavailablestock ukavailablestock\n" +
                    ", df.productinventorygertotalstock gertotalstock\n" +
                    ", df.productinventoryfraavailablestock fraavailablestock\n" +
                    ", df.productinventoryausavailablestock ausavailablestock\n" +
                    ", df.productinventoryustotalstock ustotalstock\n" +
                    ", df.productinventoryuktotalstock uktotalstock\n" +
                    ", df.productinventorygeravailablestock geravailablestock\n" +
                    ", df.productinventoryfratotalstock fratotalstock\n" +
                    ", df.productinventoryusavailablestock usavailablestock\n" +
                    ", df.productplanned planned\n" +
                    ", df.productprojectno projectno\n" +
                    ", df.productisbn isbn\n" +
                    ", df.productcontractpubdate contractpubdate\n" +
                    ", df.productmodifiedon modifiedon\n" +
                    ", df.productunitcost unitcost\n" +
                    ", df.productpublishedon publishedon\n" +
                    ", df.productdeliverystatus deliverystatus\n" +
                    ", df.productplannededitionsize plannededitionsize\n" +
                    ", df.productlatestpubdate latestpubdate\n" +
                    ", df.productmedium medium\n" +
                    ", df.productorderno orderno\n" +
                    ", df.productfirstprinting firstprinting\n" +
                    ", df.productreason reason\n" +
                    ", df.productbudgetpubdate budgetpubdate\n" +
                    ", df.productpubdateplanned pubdateplanned\n" +
                    ", df.productcreatedon createdon\n" +
                    "--, df.productexternalimpressionid externalimpressionid\n" +
                    ", df.productplannedfirstprint plannedfirstprint\n" +
                    ", df.productbinding binding\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df))\n" +
                    "where sourceref in('%s')\n" +
                    "order by sourceref desc";
    public static String getCurrentTableDataFor_stg_current_product=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "noofvolumes,refkey,externaleditionid,\n" +
                    "isbn13,versiontype,podsuitable,firstrelease,\n" +
                    "origtitle,ukavailablestock,gertotalstock,\n" +
                    "fraavailablestock,ausavailablestock,\n" +
                    "ustotalstock,uktotalstock,geravailablestock,\n" +
                    "fratotalstock,usavailablestock,planned,projectno,\n" +
                    "isbn,contractpubdate,modifiedon,unitcost,\n" +
                    "publishedon,deliverystatus,plannededitionsize,\n" +
                    "latestpubdate,medium,orderno,firstprinting,\n" +
                    "reason,budgetpubdate,pubdateplanned,createdon,\n" +
                    "--externalimpressionid,\n" +
                    "plannedfirstprint,binding\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_product \n" +
                    "where sourceref in('%s')\n" +
                    "order by sourceref desc";

    public static String randomId_ingestTableFor_stg_current_production=
            "select sourceref from(select df.productprojectno sourceref\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df)) \n" +
                    "order by rand() limit %s";
    public static String getInitialIngestDataFor_stg_current_production=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", df.productionweight weight\n" +
                    ", df.productionlineartbw lineartbw\n" +
                    ", df.coverendpaperfrontpapercolour frontpapercolour\n" +
                    ", df.coverendpaperbackpapercolour backpapercolour\n" +
                    ", df.coverendpapergrammage grammage\n" +
                    ", df.coverendpaperbackcoverpms backcoverpms\n" +
                    ", df.coverendpaperfrontcoverpms frontcoverpms\n" +
                    ", df.productionapproxpages approxpages\n" +
                    "--, df.productionreprintno reprintno\n" +
                    ", df.productionlineartcolors lineartcolors\n" +
                    ", df.productionsupplierb supplierb\n" +
                    ", df.productiontablesbw tablesbw\n" +
                    ", df.productionauthoringsystem authoringsystem\n" +
                    ", df.productiontagging tagging\n" +
                    ", df.productionsectioncolours sectioncolours\n" +
                    ", df.productionaddillustration addillustration\n" +
                    ", df.productionpagesroman pagesroman\n" +
                    ", df.productionproductionmethod productionmethod\n" +
                    ", df.productionsupplierashortname supplierashortname\n" +
                    ", df.productionsupplierafullname supplierafullname\n" +
                    ", df.productionbindmeth bindmeth\n" +
                    ", df.productionformat format\n" +
                    ", df.productiontrimsize trimsize\n" +
                    ", df.productiontrimother trimother\n" +
                    ", df.productionmstype mstype\n" +
                    ", df.productionmanuscriptpages manuscriptpages\n" +
                    ", df.productiontablescolors tablescolors\n" +
                    ", df.productionsupplierbfullname supplierbfullname\n" +
                    ", df.productionproductiondetails productiondetails\n" +
                    ", df.productionillustrationscolors illustrationscolors\n" +
                    ", df.productiongraphicscolors graphicscolors\n" +
                    ", df.productionexternalads externalads\n" +
                    ", df.productioninternalads internalads\n" +
                    ", df.productionhalftonesbw halftonesbw\n" +
                    ", df.productionmapsbw mapsbw\n" +
                    ", df.productionspinestyle spinestyle\n" +
                    ", df.productioneonlypages eonlypages\n" +
                    ", df.productionsuppliera suppliera\n" +
                    ", df.productionsupplierbshortname supplierbshortname\n" +
                    ", df.productionillustrationsbw illustrationsbw\n" +
                    ", df.productionextentprod extentprod\n" +
                    ", df.productionprintform printform\n" +
                    ", df.productionduotone duotone\n" +
                    ", df.productioncopyedlevel copyedlevel\n" +
                    ", df.coverbindingexteriorpms exteriorpms\n" +
                    ", df.coverbindingfinishing finishing\n" +
                    ", df.coverbindingmaterial material\n" +
                    ", df.coverbindingpaperquality paperquality\n" +
                    ", df.coverbindinginteriorpms interiorpms\n" +
                    "--, df.coverbindinggrammage grammage\n" +
                    ", df.coverbindinginteriorcolour interiorcolour\n" +
                    ", df.coverbindingboardthickness boardthickness\n" +
                    ", df.coverbindingspec spec\n" +
                    ", df.productionmapscolor mapscolor\n" +
                    ", df.productiontextdesigntype textdesigntype\n" +
                    ", df.productionclassification classification\n" +
                    ", df.productionhalftonescolors halftonescolors\n" +
                    ", df.productionbiblioreference biblioreference\n" +
                    ", df.productionextentstatus extentstatus\n" +
                    ", df.productionpagesarabic pagesarabic\n" +
                    ", df.productiongraphicsbw graphicsbw\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref desc";
    public static String getCurrentTableDataFor_stg_current_production=
            "select  metadeleted\n" +
                    ", metamodifiedon, sourceref, weight, lineartbw, frontpapercolour, backpapercolour\n" +
                    ", grammage, backcoverpms, frontcoverpms, approxpages, lineartcolors,supplierb\n" +
                    ", tablesbw, authoringsystem, tagging, sectioncolours, addillustration, pagesroman\n" +
                    ", productionmethod, supplierashortname, supplierafullname, bindmeth, format, trimsize\n" +
                    ", trimother, mstype, manuscriptpages, tablescolors, supplierbfullname, productiondetails\n" +
                    ", illustrationscolors, graphicscolors, externalads, internalads, halftonesbw, mapsbw\n" +
                    ", spinestyle, eonlypages, suppliera, supplierbshortname, illustrationsbw, extentprod\n" +
                    ", printform, duotone, copyedlevel, exteriorpms, finishing, material, paperquality\n" +
                    ", interiorpms, interiorcolour, boardthickness, spec, mapscolor, textdesigntype\n" +
                    ", classification, halftonescolors, biblioreference, extentstatus, pagesarabic, graphicsbw \n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_production \n" +
                    "where sourceref in ('%s')order by sourceref desc";

    public static String randomId_ingestTableFor_stg_current_relations=
            "select sourceref from(select df.productprojectno sourceref, cj.projectno projectno\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df      \n" +
                    "CROSS JOIN UNNEST(productrelation) x (cj))) \n" +
                    "where projectno is not null\n" +
                    "order by rand() limit %s";
    public static String getInitialIngestDataFor_stg_current_relations=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", cj.orderno orderno\n" +
                    ", cj.relationtype relationtype\n" +
                    ", cj.projectno projectno\n" +
                    "--, cj.relimpressionid relimpressionid\n" +
                    ", cj.isbn isbn\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df \n" +
                    "CROSS JOIN UNNEST(productrelation) x (cj)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,relationtype,projectno desc";
    public static String getCurrentTableDataFor_stg_current_relations=
            "select metadeleted,metamodifiedon,sourceref,orderno\n" +
                    ",relationtype,projectno--,relimpressionid\n" +
                    ",isbn\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_relations\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,relationtype,projectno desc";

    public static String randomId_ingestTableFor_stg_current_responsibilities=
            "select sourceref from(select \n" +
                    " df.productprojectno sourceref, cj.responsibleperson responsibleperson\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df \n" +
                    "CROSS JOIN UNNEST(contactsresponsibilities) x (cj)))\n" +
                    "where responsibleperson is not null and responsibleperson!=''\n" +
                    " order by rand() limit %s";
    public static String getInitialIngestDataFor_stg_current_responsibilities=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", cj.responsibility responsibility\n" +
                    ", cj.responsibleperson responsibleperson\n" +
                    ", cj.personid personid\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df \n" +
                    "CROSS JOIN UNNEST(contactsresponsibilities) x (cj)))\n" +
                    "where sourceref in ('%s')\n" +
                    " order by  sourceref,responsibility,responsibleperson desc";
    public static String getCurrentTableDataFor_stg_current_responsibilities=
            "select metadeleted,metamodifiedon,sourceref,responsibility\n" +
                    ",responsibleperson,personid\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_responsibilities \n" +
                    "where sourceref in ('%s')\n" +
                    "order by  sourceref,responsibility,responsibleperson desc";

    public static String randomId_ingestTableFor_stg_current_sublocation=
            "select sourceref from(select \n" +
                    " df.productprojectno sourceref\n" +
                    ", cj.warehouse warehouse\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df \n" +
                    "CROSS JOIN UNNEST(productsublocation) x (cj)))\n" +
                    "where warehouse is not null and warehouse!=''\n" +
                    "order by rand() limit %s";
    public static String getInitialIngestDataFor_stg_current_sublocation=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", cj.warehouse warehouse\n" +
                    ", cj.pubdateactual pubdateactual\n" +
                    ", cj.stocksplit stocksplit\n" +
                    ", cj.refkey refkey\n" +
                    ", cj.status status\n" +
                    ", cj.plannedpubdate plannedpubdate\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df \n" +
                    "CROSS JOIN UNNEST(productsublocation) x (cj)))\n" +
                    "where sourceref in('%s')\n" +
                    "order by sourceref,warehouse desc";

    public static String getCurrentTableDataFor_stg_current_sublocation=
            "select metadeleted,metamodifiedon,sourceref,warehouse\n" +
                    ",pubdateactual,stocksplit, refkey,status,plannedpubdate\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_sublocation\n" +
                    "where sourceref in('%s')\n" +
                    "order by sourceref,warehouse desc";


    public static String randomId_ingestTableFor_stg_current_text=
            "select sourceref from(select \n" +
                    " df.productprojectno sourceref, cj.text\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(distributiontext) x (cj)) \n" +
                    "where text is not null and text!=''\n" +
                    "order by rand() limit %s";
    public static String getInitialIngestDataFor_stg_current_text=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", cj.tab\n" +
                    ", cj.texttype\n" +
                    ", cj.name\n" +
                    ", cj.text\n" +
                    ", cj.status\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(distributiontext) x (cj))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,texttype,text desc";
    public static String getCurrentTableDataFor_stg_current_text=
            "select metadeleted,metamodifiedon,sourceref,tab\n" +
                    ",texttype,name, text,status\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_text\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,texttype,text desc";

    public static String randomId_ingestTableFor_stg_current_versionfamily=
            "select sourceref from(select \n" +
                    " df.productprojectno sourceref\n" +
                    ", cj.childprojectno childprojectno\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(contentversionfamily) x (cj))\n" +
                    "where childprojectno is not null and childprojectno!=''\n" +
                    "order by rand() limit %s";

    public static String randomId_ingestTableFor_stg_current_originatornotes=
            " select businesspartnerid from (\n" +
                    " select\n" +
                    "uo.metainfdeleted metadeleted\n" +
                    ", uo.metainfmodifiedon metamodifiedon\n" +
                    ", uo.productprojectno sourceref\n" +
                    ", uo.businesspartnerid businesspartnerid\n" +
                    ", ua.notestype notestype\n" +
                    ", ua.notes notes\n" +
                    ", ua.companygroup companygroup\n" +
                    "from ((select\n" +
                    "df.metainfdeleted\n" +
                    ", df.metainfmodifiedon\n" +
                    ",df.productprojectno\n" +
                    ", co.businesspartnerid\n" +
                    ", co.authornotes\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(contactsoriginators) x (co)))uo\n" +
                    "CROSS JOIN UNNEST(authornotes) z (ua))\n" +
                    ")order by rand() limit %s";

    public static String getInitialIngestDataFor_stg_current_originatornotes=
            " select * from (\n" +
                    " select\n" +
                    "uo.metainfdeleted metadeleted\n" +
                    ", uo.metainfmodifiedon metamodifiedon\n" +
                    ", uo.productprojectno sourceref\n" +
                    ", uo.businesspartnerid businesspartnerid\n" +
                    ", ua.notestype notestype\n" +
                    ", ua.notes notes\n" +
                    ", ua.companygroup companygroup\n" +
                    "from ((select\n" +
                    "df.metainfdeleted\n" +
                    ", df.metainfmodifiedon\n" +
                    ",df.productprojectno\n" +
                    ", co.businesspartnerid\n" +
                    ", co.authornotes\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(contactsoriginators) x (co)))uo\n" +
                    "CROSS JOIN UNNEST(authornotes) z (ua))\n" +
                    ")" +
                    "where businesspartnerid in (%s) order by businesspartnerid,notes,metamodifiedon desc";

    public static String getCurrentTableDataFor_stg_current_originatornotes=
            "select distinct metadeleted,metamodifiedon,sourceref,businesspartnerid\n" +
                    ",notestype,notes\n" +
                    " from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originatornotes\n" +
                    "where businesspartnerid in (%s)\n" +
                    "order by metamodifiedon desc";


    public static String getInitialIngestDataFor_stg_current_versionfamily=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", cj.workmasterisbn workmasterisbn\n" +
                    ", cj.workmasterprojectno workmasterprojectno\n" +
                    ", cj.childisbn childisbn\n" +
                    ", cj.childprojectno childprojectno\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_product df\n" +
                    "CROSS JOIN UNNEST(contentversionfamily) x (cj))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,childprojectno desc";

    public static String getCurrentTableDataFor_stg_current_versionfamily=
            "select metadeleted,metamodifiedon,sourceref,workmasterisbn\n" +
                    ",workmasterprojectno,childisbn,childprojectno\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_versionfamily\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,childprojectno desc";

    /*public static String GET_MANIF_INBOUND_DATA =
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
                    "   from ((((((("+GetBcsEtlCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                    "   INNER JOIN "+GetBcsEtlCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content ON (product.sourceref = content.sourceref))\n" +
                    "   INNER JOIN "+GetBcsEtlCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily workprod ON ((product.sourceref = workprod.sourceref) AND (workprod.workmasterprojectno IS NOT NULL)))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT sourceref, classificationcode \n" +
                    "      FROM "+GetBcsEtlCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification\n" +
                    "      WHERE (classificationcode LIKE 'PARELIE%')\n" +
                    "   ) intedition ON (product.sourceref = intedition.sourceref))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT sourceref, min(plannedpubdate) firstplanned \n" +
                    "      FROM "+GetBcsEtlCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation\n" +
                    "      WHERE (plannedpubdate <> '') GROUP BY sourceref \n" +
                    "   ) planneddates ON (product.sourceref = planneddates.sourceref))\n" +
                    "   LEFT JOIN (\n" +
                    "      select sourceref, min(pubdateactual) firstactual \n" +
                    "      FROM "+GetBcsEtlCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation\n" +
                    "      WHERE (pubdateactual <> '') GROUP BY sourceref \n" +
                    "   )  actualdates ON (product.sourceref = actualdates.sourceref))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT distinct sourceref, ephmanifestationcode \n" +
                    "      FROM ((SELECT sourceref, min(priority) statuspriority \n" +
                    "         FROM ("+GetBcsEtlCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation\n" +
                    "         INNER JOIN "+GetBcsEtlCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (split_part(status, ' | ', 1) = ppmcode))\n" +
                    "         GROUP BY sourceref)  masterstatus\n" +
                    "      INNER JOIN "+GetBcsEtlCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (statuspriority = priority))\n" +
                    "   )  manifestationstatus ON (product.sourceref = manifestationstatus.sourceref))\n" +
                    "   LEFT JOIN "+GetBcsEtlCoreDLDBUser.getBCS_ETLCoreDataBase()+".manifestationtypecode ON (split_part(product.versiontype, ' | ', 1) = manifestationtypecode.ppmcode))\n" +
                    ") where sourceref in ('%s') order by sourceref desc";
                    */

    public static String randomId_stg_current_classification=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_classification \n" +
                    "where value is not null AND value!='' AND value!='0' order by rand() limit 1";
    public static String getData_stg_current_classification=
            "select metadeleted, metamodifiedon,sourceref,classificationcode,\n" +
                    "value,classificationtype,priority,businessunit\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_classification \n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,value,classificationcode desc";
    public static String getData_stg_history_classification_part=
            "select metadeleted, metamodifiedon,sourceref,classificationcode,\n" +
                    "value,classificationtype,priority,businessunit\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_classification_part \n" +
                    "where sourceref in ('%s') and \n" +
                    "inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_classification_part)\n" +
                    "order by sourceref,value,classificationcode desc";

    public static String randomId_stg_current_content=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_content order by rand() limit %s";
    public static String getData_stg_current_content=
          "select metadeleted,metamodifiedon,sourceref,\n" +
                  "originimpid,subgroup,series,copyrightyear,\n" +
                  "title,worktitle,isset,doistatus,\n" +
                  "imprint,division,language3,regstatus,approvedondate,\n" +
                  "language2,doi,volumeno,editionid,synctemplate,\n" +
                  "volumename,seriesissn,seriesid,ownership,\n" +
                  "firstapproval,companygroup,shorttitle,editionno,\n" +
                  "work_master_flag,language,piidack,publisher,\n" +
                  "titleid,subtitle,seriescode,objecttype\n" +
                  "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_content \n" +
                  "where sourceref in ('%s')\n" +
                  "order by sourceref desc";
    public static String getData_stg_history_content_part=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "originimpid,subgroup,series,copyrightyear,\n" +
                    "title,worktitle,isset,doistatus,\n" +
                    "imprint,division,language3,regstatus,approvedondate,\n" +
                    "language2,doi,volumeno,editionid,synctemplate,\n" +
                    "volumename,seriesissn,seriesid,ownership,\n" +
                    "firstapproval,companygroup,shorttitle,editionno,\n" +
                    "work_master_flag,language,piidack,publisher,\n" +
                    "titleid,subtitle,seriescode,objecttype\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_content_part\n" +
                    "where sourceref in ('%s')\n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_content_part)\n" +
                    "order by sourceref desc";

    public static String randomId_stg_current_extobject=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_extobject \n" +
                    "where name is not null AND name!='' order by rand() limit %s";
    public static String getData_stg_current_extobject=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "object,type,name,comments,source \n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_extobject \n" +
                    "where sourceref in ('%s') \n" +
                    "order by sourceref,name,object desc";
    public static String getData_stg_history_extobject_part=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "object,type,name,comments,source \n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_extobject_part\n" +
                    "where sourceref in ('%s') \n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_extobject_part)\n" +
                    "order by sourceref,name,object desc";

    public static String randomId_stg_current_fullversionfamily=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_fullversionfamily where projectno is not null AND projectno!='' order by rand() limit %s";

    public static String getData_stg_current_fullversionfamily=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "versiontype,editionno,isbn,projectno,workmaster\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_fullversionfamily \n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,projectno desc";

    public static String getData_stg_history_fullversionfamily_part=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "versiontype,editionno,isbn,projectno,workmaster\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_fullversionfamily_part \n" +
                    "where sourceref in ('%s')\n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_fullversionfamily_part)\n" +
                    "order by sourceref,projectno desc";

    public static String randomId_stg_current_originatoraddress =
            "select businesspartnerid from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originatoraddress where businesspartnerid is not null order by rand() limit %s";

    public static String randomId_stg_current_originatornotes =
            "select businesspartnerid from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originatornotes where businesspartnerid is not null order by rand() limit %s";

    public static String randomId_stg_current_originators=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originators where businesspartnerid is not null order by rand() limit %s";

    public static String getData_stg_current_originators=
            "select metadeleted,metamodifiedon,sourceref, prefix,sequence,businesspartnerid,originatorid,\n" +
                    "isperson,locationid,copyrightholdertype,institution,\n" +
                    "firstname,department,lastname,searchterm\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originators \n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,copyrightholdertype,businesspartnerid desc";


    public static String getData_stg_history_originators_part=
            "select metadeleted,metamodifiedon,sourceref, prefix,sequence,businesspartnerid,originatorid,\n" +
                    "isperson,locationid,copyrightholdertype,institution,\n" +
                    "firstname,department,lastname,searchterm\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originators_part \n" +
                    "where sourceref in ('%s')\n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originators_part)\n" +
                    "order by sourceref,copyrightholdertype,businesspartnerid desc";

    public static String getData_stg_history_originatornotes_part=
            "select distinct metadeleted,metamodifiedon,sourceref,businesspartnerid\n" +
                    ",notestype,notes\n" +
                    " from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originatornotes_part\n" +
                    "where businesspartnerid in (%s)\n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originatornotes_part)\n" +
                    "order by metamodifiedon desc";


    public static String getData_stg_history_originatoraddress_part=
            "select metadeleted,metamodifiedon,businesspartnerid,addressline2,\n" +
                    "country,fax,street,additionaladdress,city,addressline1,addressid,telephonemain,\n" +
                    "ismainaddress,internet,houseno,email,addressline3,telephoneother,postalcode,\n" +
                    "mobile,district from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originatoraddress_part \n" +
                    "where businesspartnerid in (%s)\n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originatoraddress_part)\n" +
                    "order by businesspartnerid,country,metamodifiedon desc";

    public static String randomId_stg_current_pricing=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_pricing \n" +
                    "where validto is not null and validto !='' order by rand() limit %s";
    public static String getData_stg_current_pricing=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "validfrom,type,currency,priceapprox,price,validto\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_pricing\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,type,currency,validto desc";
    public static String getData_stg_history_pricing_part =
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "validfrom,type,currency,priceapprox,price,validto\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_pricing_part\n" +
                    "where sourceref in ('%s')\n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_pricing_part)\n" +
                    "order by sourceref,type,currency,validto desc";
    public static String randomId_stg_current_product=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_product order by rand() limit %s";
    public static String getData_stg_current_product=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "noofvolumes,refkey,externaleditionid,\n" +
                    "isbn13,versiontype,podsuitable,firstrelease,\n" +
                    "origtitle,ukavailablestock,gertotalstock,\n" +
                    "fraavailablestock,ausavailablestock,\n" +
                    "ustotalstock,uktotalstock,geravailablestock,\n" +
                    "fratotalstock,usavailablestock,planned,projectno,\n" +
                    "isbn,contractpubdate,modifiedon,unitcost,\n" +
                    "publishedon,deliverystatus,plannededitionsize,\n" +
                    "latestpubdate,medium,orderno,firstprinting,\n" +
                    "reason,budgetpubdate,pubdateplanned,createdon,\n" +
                    "--externalimpressionid,\n plannedfirstprint,binding\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_product \n" +
                    "where sourceref in('%s')\n" +
                    "order by sourceref desc";
    public static String getData_stg_history_product_part=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "noofvolumes,refkey,externaleditionid,\n" +
                    "isbn13,versiontype,podsuitable,firstrelease,\n" +
                    "origtitle,ukavailablestock,gertotalstock,\n" +
                    "fraavailablestock,ausavailablestock,\n" +
                    "ustotalstock,uktotalstock,geravailablestock,\n" +
                    "fratotalstock,usavailablestock,planned,projectno,\n" +
                    "isbn,contractpubdate,modifiedon,unitcost,\n" +
                    "publishedon,deliverystatus,plannededitionsize,\n" +
                    "latestpubdate,medium,orderno,firstprinting,\n" +
                    "reason,budgetpubdate,pubdateplanned,createdon,\n" +
                    "--externalimpressionid,\n" +
                    "plannedfirstprint,binding\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_product_part \n" +
                    "where sourceref in('%s')\n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_product_part)\n" +
                    "order by sourceref desc";

    public static String randomId_stg_current_production=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_production order by rand() limit %s";
    public static String getData_stg_current_production=
            "select  metadeleted\n" +
                    ", metamodifiedon, sourceref, weight, lineartbw, frontpapercolour, backpapercolour\n" +
                    ", grammage, backcoverpms, frontcoverpms, approxpages, --reprintno,\n lineartcolors,supplierb\n" +
                    ", tablesbw, authoringsystem, tagging, sectioncolours, addillustration, pagesroman\n" +
                    ", productionmethod, supplierashortname, supplierafullname, bindmeth, format, trimsize\n" +
                    ", trimother, mstype, manuscriptpages, tablescolors, supplierbfullname, productiondetails\n" +
                    ", illustrationscolors, graphicscolors, externalads, internalads, halftonesbw, mapsbw\n" +
                    ", spinestyle, eonlypages, suppliera, supplierbshortname, illustrationsbw, extentprod\n" +
                    ", printform, duotone, copyedlevel, exteriorpms, finishing, material, paperquality\n" +
                    ", interiorpms, interiorcolour, boardthickness, spec, mapscolor, textdesigntype\n" +
                    ", classification, halftonescolors, biblioreference, extentstatus, pagesarabic, graphicsbw \n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_production \n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref desc";
    public static String getData_stg_history_production_part=
            "select  metadeleted\n" +
                    ", metamodifiedon, sourceref, weight, lineartbw, frontpapercolour, backpapercolour\n" +
                    ", grammage, backcoverpms, frontcoverpms, approxpages, --reprintno,\n lineartcolors,supplierb\n" +
                    ", tablesbw, authoringsystem, tagging, sectioncolours, addillustration, pagesroman\n" +
                    ", productionmethod, supplierashortname, supplierafullname, bindmeth, format, trimsize\n" +
                    ", trimother, mstype, manuscriptpages, tablescolors, supplierbfullname, productiondetails\n" +
                    ", illustrationscolors, graphicscolors, externalads, internalads, halftonesbw, mapsbw\n" +
                    ", spinestyle, eonlypages, suppliera, supplierbshortname, illustrationsbw, extentprod\n" +
                    ", printform, duotone, copyedlevel, exteriorpms, finishing, material, paperquality\n" +
                    ", interiorpms, interiorcolour, boardthickness, spec, mapscolor, textdesigntype\n" +
                    ", classification, halftonescolors, biblioreference, extentstatus, pagesarabic, graphicsbw \n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_production_part \n" +
                    "where sourceref in ('%s')\n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_production_part)\n" +
                    "order by sourceref desc";

    public static String randomId_stg_current_relations=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_relations \n" +
                    "where projectno is not null and projectno!=''\n" +
                    "order by rand() limit %s";
    public static String getData_stg_current_relations=
            "select metadeleted,metamodifiedon,sourceref,orderno\n" +
                    ",relationtype,projectno,--relimpressionid,\n isbn\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_relations\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,relationtype,projectno desc";
    public static String getData_stg_history_relations_part=
            "select metadeleted,metamodifiedon,sourceref,orderno\n" +
                    ",relationtype,projectno,--relimpressionid,\n isbn\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_relations_part\n" +
                    "where sourceref in ('%s')\n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_relations_part)\n" +
                    "order by sourceref,relationtype,projectno desc";

    public static String randomId_stg_current_responsibilities=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_responsibilities where responsibleperson is not null and responsibleperson!=''\n" +
                    "order by rand() limit %s";
    public static String getData_stg_current_responsibilities=
            "select metadeleted,metamodifiedon,sourceref,responsibility\n" +
                    ",responsibleperson,personid\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_responsibilities \n" +
                    "where sourceref in ('%s')\n" +
                    "order by  sourceref,responsibility,responsibleperson desc";
    public static String getData_stg_history_responsibilities_part=
            "select metadeleted,metamodifiedon,sourceref,responsibility\n" +
                    ",responsibleperson,personid\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_responsibilities_part \n" +
                    "where sourceref in ('%s') and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_responsibilities_part)\n" +
                    "order by  sourceref,responsibility,responsibleperson desc";

    public static String randomId_stg_current_sublocation=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_sublocation where warehouse is not null and warehouse!=''\n" +
                    "order by rand() limit %s";
    public static String getData_stg_current_sublocation=
            "select metadeleted,metamodifiedon,sourceref,warehouse\n" +
                    ",pubdateactual,stocksplit, refkey,status,plannedpubdate\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_sublocation\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,warehouse desc";
    public static String getData_stg_history_sublocation_part=
            "select metadeleted,metamodifiedon,sourceref,warehouse\n" +
                    ",pubdateactual,stocksplit, refkey,status,plannedpubdate\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_sublocation_part\n" +
                    "where sourceref in ('%s')\n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_sublocation_part)\n" +
                    "order by sourceref,warehouse desc";
    public static String randomId_stg_current_text=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_text \n" +
                    "where text is not null and text!=''\n" +
                    "order by rand() limit %s";
    public static String getData_stg_current_text=
            "select metadeleted,metamodifiedon,sourceref,tab\n" +
                    ",texttype,name, text,status\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_text\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,texttype,text desc";
    public static String getData_stg_history_text_part=
            "select metadeleted,metamodifiedon,sourceref,tab\n" +
                    ",texttype,name, text,status\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_text_part\n" +
                    "where sourceref in ('%s')\n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_text_part)\n" +
                    "order by sourceref,texttype,text desc";


    public static String randomId_stg_current_versionfamily=
            "select sourceref from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_versionfamily where childprojectno is not null and childprojectno!=''\n" +
                    "order by rand() limit %s";
    public static String getData_stg_current_versionfamily=
            "select metadeleted,metamodifiedon,sourceref,workmasterisbn\n" +
                    ",workmasterprojectno,childisbn, childprojectno\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_versionfamily\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,childprojectno desc";
    public static String getData_stg_history_versionfamily_part=
            "select metadeleted,metamodifiedon,sourceref,workmasterisbn\n" +
                    ",workmasterprojectno,childisbn, childprojectno\n" +
                    "from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_versionfamily_part\n" +
                    "where sourceref in ('%s')\n" +
                    "and inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_versionfamily_part)\n" +
                    "order by sourceref,childprojectno desc";

    public static String randomId_ingestTableFor_stg_current_classification_series =
            "select distinct sourceref from(SELECT metainfdeleted metadeleted, metainfmodifiedon metamodifiedon, contentseriesid sourceref, cl.businessunit, cl.value\n" +
                    ", cl.classificationtype, cl.priority, cl.classificationcode FROM\n" +
                    "(bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series f\n" +
                    "CROSS JOIN UNNEST(distributionclassification) x (cl))) limit %s";

    public static String randomId_ingestTableFor_stg_current_content_series =
            "select distinct sourceref from(SELECT df.metainfdeleted metadeleted,df.metainfmodifiedon metamodifiedon,df.contentseriesid sourceref,df.contentsubgroup subgroup,df.contentseriescode seriescode,df.contentmedium medium\n" +
                    ",df.contentwmyn wmyn,df.contentsubtitle subtitle,df.contenttitle title,df.contentserialtype serialtype,df.contentdivision division,df.contentobjType objType\n" +
                    ",df.contentcompanygroup companygroup,df.contentseriesissn seriesissn\n" +
                    ",df.contentfirstbinding binding,df.contentvolumeno volumeno,df.contentlanguage language,df.contentpublisher publisher,df.contentseriesid seriesid,df.contentshortTitle shorttitle,df.contentpiidack piidack,df.contentownership ownership,df.contentdeltype deltype,df.contentnumbered numbered,df.contentbibliographicserial bibliographicserial\n" +
                    ",df.contentmainseries mainseries,df.contenteditionid editionid FROM\n" +
                    "bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df) limit %s";

    public static String randomId_ingestTableFor_stg_current_originatoraddress_series =
            "select distinct businesspartnerid from (select distinct uo.metainfdeleted metadeleted, uo.metainfmodifiedon metamodifiedon, uo.businesspartnerid businesspartnerid\n" +
                    ", ua.country country, ua.postalcode postalcode, ua.additionaladdress additionaladdress, ua.houseno houseno\n" +
                    ", ua.internet internet, ua.city city, ua.street street, ua.email email, ua.district district\n" +
                    ", ua.mobile mobile, ua.fax fax, ua.telephoneother telephoneother, ua.telephonemain telephonemain\n" +
                    "from ((select df.metainfdeleted , df.metainfmodifiedon , co.businesspartnerid , co.authoraddress\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df CROSS JOIN UNNEST(contactsoriginators) x (co)))uo\n" +
                    "CROSS JOIN UNNEST(authoraddress) z (ua))) limit %s";

    public static String randomId_ingestTableFor_stg_current_originatornotes_series =
            "select distinct businesspartnerid from (select distinct  uo.metainfdeleted metadeleted, uo.metainfmodifiedon metamodifiedon,\n" +
                    "uo.contentseriesid sourceref, uo.businesspartnerid businesspartnerid, ua.notes notes,\n" +
                    "ua.companygroup companygroup from  ((select df.metainfdeleted , df.metainfmodifiedon ,df.contentseriesid, co.businesspartnerid ,\n" +
                    "co.authornotes from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df  CROSS JOIN UNNEST(contactsoriginators) x (co)))uo CROSS JOIN UNNEST(authornotes) z (ua)))\n" +
                    " limit %s";

    public static String randomId_ingestTableFor_stg_current_originator_series =
            "select distinct businesspartnerid from" +
                    "(select\n" +
                    "df.metainfdeleted metadeleted, df.metainfmodifiedon metamodifiedon\n" +
                    ", df.contentseriesid sourceref, co.firstname firstname\n" +
                    ", co.businesspartnerid businesspartnerid, co.lastname lastname\n" +
                    ", co.sequence sequence, co.prefix prefix\n" +
                    ", co.copyrightholdertype copyrightholdertype, co.searchterm searchterm\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df CROSS JOIN UNNEST(contactsoriginators) x (co))) limit %s";

    public static String randomId_ingestTableFor_stg_current_product_series =
            "select distinct sourceref from(select df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon, df.contentseriesid sourceref\n" +
                    ", df.productorderno orderno, df.productversiontype versiontype\n" +
                    "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df)) limit %s";

    public static String randomId_ingestTableFor_stg_current_text_series =
            "select distinct sourceref from(\n" +
                    "select df.metainfdeleted metadeleted, df.metainfmodifiedon metamodifiedon\n" +
                    ", df.contentseriesid sourceref, cj.tab, cj.texttype, cj.name, cj.text\n" +
                    ", cj.status from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df \n" +
                    "CROSS JOIN UNNEST(distributiontext) x (cj)) limit %s";

    public static String getInitialIngestDataFor_stg_current_classification_series_rec =
            "select metadeleted as metadeleted" +
                    ",metamodifiedon as metamodifiedon" +
                    ",sourceref as sourceref" +
                    ",businessunit as businessunit" +
                    ",classificationtype as classificationtype" +
                    ",priority as priority" +
                    ",classificationcode as classificationcode" +
                    ",value as value" +
                    " from(SELECT metainfdeleted metadeleted, metainfmodifiedon metamodifiedon, contentseriesid sourceref, cl.businessunit, cl.value\n" +
                    ", cl.classificationtype, cl.priority, cl.classificationcode FROM\n" +
                    "(bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series f\n" +
                    "CROSS JOIN UNNEST(distributionclassification) x (cl)))  where sourceref in ('%s') order by sourceref,metamodifiedon,classificationtype,classificationcode desc";

    public static String getCurrentTableDataFor_stg_current_classification_series =
            "select metadeleted as metadeleted" +
                    ",metamodifiedon as metamodifiedon" +
                    ",sourceref as sourceref" +
                    ",businessunit as businessunit" +
                    ",classificationtype as classificationtype" +
                    ",priority as priority" +
                    ",classificationcode as classificationcode" +
                    ",value as value" +
                    " from" +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_classification_series" +
                    " where sourceref in ('%s') order by sourceref,metamodifiedon,classificationtype,classificationcode desc";

    public static String getInitialIngestDataFor_stg_current_content_series_rec =
           "select metadeleted as metadeleted" +
                   ",metamodifiedon as metamodifiedon" +
                   ",sourceref as sourceref" +
                   ",subgroup as subgroup" +
                   ",seriescode as seriescode" +
                   ",medium as medium" +
                   ",wmyn as wmyn" +
                   ",subtitle as subtitle" +
                   ",title as title" +
                   ",serialtype as serialtype" +
                   ",division as division" +
                   ",objtype as objtype" +
                   ",companygroup as companygroup" +
                   ",seriesissn as seriesissn" +
                   ",binding as binding" +
                   ",volumeno as volumeno" +
                   ",language as language" +
                   ",publisher as publisher" +
                   ",seriesid as seriesid" +
                   ",shorttitle as shorttitle" +
                   ",piidack as piidack" +
                   ",ownership as ownership" +
                   ",deltype as deltype" +
                   ",numbered as numbered" +
                   ",bibliographicserial as bibliographicserial" +
                   ",mainseries as mainseries" +
                   ",editionid as editionid " +
                   " from(SELECT df.metainfdeleted metadeleted,df.metainfmodifiedon metamodifiedon,df.contentseriesid sourceref,df.contentsubgroup subgroup\n" +
                   ",df.contentseriescode seriescode,df.contentmedium medium,df.contentwmyn wmyn\n" +
                   ",df.contentsubtitle subtitle,df.contenttitle title,df.contentserialtype serialtype\n" +
                   ",df.contentdivision division,df.contentobjType objType,df.contentcompanygroup companygroup,df.contentseriesissn seriesissn,df.contentfirstbinding binding\n" +
                   ",df.contentvolumeno volumeno,df.contentlanguage language\n" +
                   ",df.contentpublisher publisher,df.contentseriesid seriesid,df.contentshortTitle shorttitle\n" +
                   ",df.contentpiidack piidack,df.contentownership ownership,df.contentdeltype deltype\n" +
                   ",df.contentnumbered numbered,df.contentbibliographicserial bibliographicserial\n" +
                   ",df.contentmainseries mainseries,df.contenteditionid editionid FROM\n" +
                   " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df)where sourceref in ('%s') order by sourceref,seriescode,editionid desc";

            public static String getCurrentTableDataFor_stg_current_content_series =
                    "select metadeleted as metadeleted" +
                            ",metamodifiedon as metamodifiedon" +
                            ",sourceref as sourceref" +
                            ",subgroup as subgroup" +
                            ",seriescode as seriescode" +
                            ",medium as medium" +
                            ",wmyn as wmyn" +
                            ",subtitle as subtitle" +
                            ",title as title" +
                            ",serialtype as serialtype" +
                            ",division as division" +
                            ",objtype as objtype" +
                            ",companygroup as companygroup" +
                            ",seriesissn as seriesissn" +
                            ",binding as binding" +
                            ",volumeno as volumeno" +
                            ",language as language" +
                            ",publisher as publisher" +
                            ",seriesid as seriesid" +
                            ",shorttitle as shorttitle" +
                            ",piidack as piidack" +
                            ",ownership as ownership" +
                            ",deltype as deltype" +
                            ",numbered as numbered" +
                            ",bibliographicserial as bibliographicserial" +
                            ",mainseries as mainseries" +
                            ",editionid as editionid " +
                            " from" +
                            " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_content_series where sourceref in ('SERI-%s') order by sourceref,seriescode,editionid desc\n";

            public static String getInitialIngestDataFor_stg_current_originatoraddress_series_rec =
                    "select metadeleted as metadeleted\n" +
                            ",metamodifiedon as metamodifiedon\n" +
                            ",businesspartnerid as businesspartnerid\n" +
                            ",country as country\n" +
                            ",postalcode as postalcode\n" +
                            ",additionaladdress as additionaladdress\n" +
                            ",houseno as houseno \n" +
                            ",internet as internet\n" +
                            ",city as city\n" +
                            ",street as street\n" +
                            ",email as email\n" +
                            ",district as district\n" +
                            ",mobile as mobile\n" +
                            ",fax as fax\n" +
                            ",telephoneother as telephoneother\n" +
                            ",telephonemain as telephonemain" +
                            " from (select distinct uo.metainfdeleted metadeleted, uo.metainfmodifiedon metamodifiedon, uo.businesspartnerid businesspartnerid\n" +
                            ", ua.country country, ua.postalcode postalcode, ua.additionaladdress additionaladdress, ua.houseno houseno\n" +
                            ", ua.internet internet, ua.city city, ua.street street, ua.email email, ua.district district\n" +
                            ", ua.mobile mobile, ua.fax fax, ua.telephoneother telephoneother, ua.telephonemain telephonemain\n" +
                            "from ((select df.metainfdeleted , df.metainfmodifiedon , co.businesspartnerid , co.authoraddress\n" +
                            "from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df CROSS JOIN UNNEST(contactsoriginators) x (co)))uo\n" +
                            "CROSS JOIN UNNEST(authoraddress) z (ua))) where businesspartnerid in (%s) order by businesspartnerid desc";

            public static String getCurrentTableDataFor_stg_current_originatoraddress_series =
                    "select metadeleted as metadeleted\n" +
                            ",metamodifiedon as metamodifiedon\n" +
                            ",businesspartnerid as businesspartnerid\n" +
                            ",country as country\n" +
                            ",postalcode as postalcode\n" +
                            ",additionaladdress as additionaladdress\n" +
                            ",houseno as houseno \n" +
                            ",internet as internet\n" +
                            ",city as city\n" +
                            ",street as street\n" +
                            ",email as email\n" +
                            ",district as district\n" +
                            ",mobile as mobile\n" +
                            ",fax as fax\n" +
                            ",telephoneother as telephoneother\n" +
                            ",telephonemain as telephonemain" +
                            " from " +
                            " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originatoraddress_series where businesspartnerid in (%s) order by businesspartnerid desc";

            public static String getInitialIngestDataFor_stg_current_originatornotes_series_rec =
                    "select metadeleted as metadeleted" +
                            ",metamodifiedon as metamodifiedon" +
                            ",businesspartnerid  as businesspartnerid " +
                            ",sourceref as sourceref" +
                        //    ",notestype as notestype" +
                            ",notes as notes" +
                            ",companygroup as companygroup" +
                            " from (select distinct  uo.metainfdeleted metadeleted, uo.metainfmodifiedon metamodifiedon,\n" +
                            "uo.contentseriesid sourceref, uo.businesspartnerid businesspartnerid, ua.notes notes,\n" +
                            "ua.companygroup companygroup from ((select df.metainfdeleted , df.metainfmodifiedon ,df.contentseriesid, co.businesspartnerid ,\n" +
                            "co.authornotes from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df  CROSS JOIN UNNEST(contactsoriginators) x (co)))uo CROSS JOIN UNNEST(authornotes) z (ua)))\n" +
                            "where businesspartnerid in (%s) order by businesspartnerid,notes desc";

            public static String getCurrentTableDataFor_stg_current_originatornotes_series =
                    "select metadeleted as metadeleted" +
                            ",metamodifiedon as metamodifiedon" +
                            ",businesspartnerid  as businesspartnerid " +
                            ",sourceref as sourceref" +
                           // ",notestype as notestype" +
                            ",notes as notes" +
                            ",companygroup as companygroup" +
                            " from " +
                            " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originatornotes_series where businesspartnerid in (%s) order by businesspartnerid,notes desc";


            public static String getInitialIngestDataFor_stg_current_originators_series_rec =
                    "select metadeleted as metadeleted \n" +
                            ",metamodifiedon as metamodifiedon\n" +
                            ",sourceref as sourceref \n" +
                            ",firstname as firstname\n" +
                            ",businesspartnerid as businesspartnerid\n" +
                            ",lastname as lastname\n" +
                            ",sequence as sequence\n" +
                            ",prefix as prefix\n" +
                            ",copyrightholdertype as copyrightholdertype\n" +
                            ",searchterm as searchterm" +
                            " from(\n" +
                            "(select\n" +
                            "\"df\".\"metainfdeleted\" \"metadeleted\", \"df\".\"metainfmodifiedon\" \"metamodifiedon\"\n" +
                            ", \"df\".\"contentseriesid\" \"sourceref\", \"co\".\"firstname\"\t\"firstname\", \"co\".\"businesspartnerid\"\t\"businesspartnerid\", \"co\".\"lastname\"\t\"lastname\", \"co\".\"sequence\"\t\"sequence\"\n" +
                            ", \"co\".\"prefix\"\t\"prefix\", \"co\".\"copyrightholdertype\"\t\"copyrightholdertype\"\n" +
                            ", \"co\".\"searchterm\"\t\"searchterm\"  from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df\n" +
                            "CROSS JOIN UNNEST(\"contactsoriginators\") x (\"co\")))) where businesspartnerid in (%s) order by businesspartnerid,sourceref,metamodifiedon desc";

            public static String getCurrentTableDataFor_stg_current_originators_series=
                    "select metadeleted as metadeleted \n" +
                            ",metamodifiedon as metamodifiedon\n" +
                            ",sourceref as sourceref \n" +
                            ",firstname as firstname\n" +
                            ",businesspartnerid as businesspartnerid\n" +
                            ",lastname as lastname\n" +
                            ",sequence as sequence\n" +
                            ",prefix as prefix\n" +
                            ",copyrightholdertype as copyrightholdertype\n" +
                            ",searchterm as searchterm" +
                            " from " +
                            "bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originators_series where businesspartnerid in (%s) order by businesspartnerid,sourceref,metamodifiedon desc";

            public static String getInitialIngestDataFor_stg_current_product_series_rec =
                    "select metadeleted as metadeleted" +
                            ",metamodifiedon as metamodifiedon" +
                            ",sourceref as sourceref" +
                            ",orderno as orderno" +
                            ",versiontype as versiontype" +
                            " from(select df.metainfdeleted metadeleted, df.metainfmodifiedon metamodifiedon,\n" +
                            "df.contentseriesid sourceref, df.productorderno orderno, df.productversiontype \n" +
                            "versiontype from (bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df))\n" +
                            "where sourceref in ('%s') order by sourceref,orderno desc\n";

            public static String getCurrentTableDataFor_stg_current_product_series =
                    "select metadeleted as metadeleted" +
                            ",metamodifiedon as metamodifiedon" +
                            ",sourceref as sourceref" +
                            ",orderno as orderno" +
                            ",versiontype as versiontype" +
                            " from " +
                            "bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_product_series where sourceref in ('%s') order by sourceref,orderno desc\n";

            public static String getInitialIngestDataFor_stg_current_text_series_rec  =
                    "select metadeleted as metadeleted" +
                            ",metamodifiedon as metamodifiedon" +
                            ",sourceref as sourceref" +
                            ",tab as tab" +
                            ",texttype as texttype" +
                            ",text as text" +
                            ",status as status" +
                            ",name as name" +
                            " from(select\n" +
                            "df.metainfdeleted metadeleted, df.metainfmodifiedon metamodifiedon\n" +
                            ", df.contentseriesid sourceref, cj.tab, cj.texttype\n" +
                            ", cj.text, cj.status, cj.name from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".initial_ingest_series df \n" +
                            "CROSS JOIN UNNEST(distributiontext) x (cj))\n" +
                            "where sourceref in ('%s') order by sourceref,name desc";

            public static String getCurrentTableDataFor_stg_current_text_series =
                    "select metadeleted as metadeleted" +
                            ",metamodifiedon as metamodifiedon" +
                            ",sourceref as sourceref" +
                            ",tab as tab" +
                            ",texttype as texttype" +
                            ",text as text" +
                            ",status as status" +
                            ",name as name" +
                            " from "    +
                            " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_text_series where sourceref in ('%s') order by sourceref,name desc";


    public static String randomId_stg_current_classification_series =
     "select distinct sourceref"+
             " from" +
             " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_classification_series" +
             " limit %s";

    public static String randomId_stg_current_content_series =
            "select distinct sourceref"+
                    " from" +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_content_series" +
                    " limit %s";

    public static String randomId_stg_current_originatoraddress_series =
            "select distinct businesspartnerid"+
                    " from" +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originatoraddress_series" +
                    " limit %s";
    public static String randomId_stg_current_originatornotes_series =
            "select distinct businesspartnerid"+
                    " from" +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originatornotes_series" +
                    " limit %s";
    public static String randomId_stg_current_originator_series =
            "select distinct businesspartnerid"+
                    " from" +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_originators_series" +
                    " limit %s";
    public static String randomId_stg_current_product_series =
            "select distinct sourceref"+
                    " from" +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_product_series" +
                    " limit %s";
    public static String randomId_stg_current_text_series =
            "select distinct sourceref"+
                    " from" +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_current_text_series" +
                    " limit %s";


    public static String getCurrentHistoryTableDataFor_stg_current_classification_series =
            "select metadeleted as metadeleted" +
                    ",metamodifiedon as metamodifiedon" +
                    ",sourceref as sourceref" +
                    ",businessunit as businessunit" +
                    ",classificationtype as classificationtype" +
                    ",priority as priority" +
                    ",classificationcode as classificationcode" +
                    ",value as value" +
                    " from" +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_classification_series_part" +
                    " where sourceref in ('%s') and \n" +
                    " inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_classification_series_part)\n" +
                    " order by sourceref,metamodifiedon,classificationtype,classificationcode desc";

    public static String getHistoryTableDataFor_stg_current_content_series =
            "select metadeleted as metadeleted" +
                    ",metamodifiedon as metamodifiedon" +
                    ",sourceref as sourceref" +
                    ",subgroup as subgroup" +
                    ",seriescode as seriescode" +
                    ",medium as medium" +
                    ",wmyn as wmyn" +
                    ",subtitle as subtitle" +
                    ",title as title" +
                    ",serialtype as serialtype" +
                    ",division as division" +
                    ",objtype as objtype" +
                    ",companygroup as companygroup" +
                    ",seriesissn as seriesissn" +
                    ",binding as binding" +
                    ",volumeno as volumeno" +
                    ",language as language" +
                    ",publisher as publisher" +
                    ",seriesid as seriesid" +
                    ",shorttitle as shorttitle" +
                    ",piidack as piidack" +
                    ",ownership as ownership" +
                    ",deltype as deltype" +
                    ",numbered as numbered" +
                    ",bibliographicserial as bibliographicserial" +
                    ",mainseries as mainseries" +
                    ",editionid as editionid " +
                    " from" +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_content_series_part" +
                    " where sourceref in ('%s') and \n" +
                    " inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_content_series_part)\n" +
                    " order by sourceref,seriescode,editionid desc\n";

    public static String getHistoryTableDataFor_stg_current_originatoraddress_series =
            "select metadeleted as metadeleted\n" +
                    ",metamodifiedon as metamodifiedon\n" +
                    ",businesspartnerid as businesspartnerid\n" +
                    ",country as country\n" +
                    ",postalcode as postalcode\n" +
                    ",additionaladdress as additionaladdress\n" +
                    ",houseno as houseno \n" +
                    ",internet as internet\n" +
                    ",city as city\n" +
                    ",street as street\n" +
                    ",email as email\n" +
                    ",district as district\n" +
                    ",mobile as mobile\n" +
                    ",fax as fax\n" +
                    ",telephoneother as telephoneother\n" +
                    ",telephonemain as telephonemain" +
                    " from " +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originatoraddress_series_part" +
                    " where businesspartnerid in (%s) and" +
                    " inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originatoraddress_series_part)\n" +
                    "order by businesspartnerid desc";

    public static String getHistTableDataFor_stg_current_originators_series=
            "select metadeleted as metadeleted \n" +
                    ",metamodifiedon as metamodifiedon\n" +
                    ",sourceref as sourceref \n" +
                    ",firstname as firstname\n" +
                    ",businesspartnerid as businesspartnerid\n" +
                    ",lastname as lastname\n" +
                    ",sequence as sequence\n" +
                    ",prefix as prefix\n" +
                    ",copyrightholdertype as copyrightholdertype\n" +
                    ",searchterm as searchterm" +
                    " from " +
                    "bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originators_series_part" +
                    " where businesspartnerid in (%s) and" +
                    " inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originators_series_part)\n" +
                    " order by businesspartnerid,sourceref,metamodifiedon desc";

    public static String getHistoryTableDataFor_stg_current_originatornotes_series =
            "select metadeleted as metadeleted" +
                    ",metamodifiedon as metamodifiedon" +
                    ",businesspartnerid  as businesspartnerid " +
                    ",sourceref as sourceref" +
                    // ",notestype as notestype" +
                    ",notes as notes" +
                    ",companygroup as companygroup" +
                    " from " +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originatornotes_series_part" +
                    " where businesspartnerid in (%s) and" +
                    " inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_originatornotes_series_part)\n" +
                    " order by businesspartnerid,notes desc";

    public static String getHistoryTableDataFor_stg_current_product_series =
            "select metadeleted as metadeleted" +
                    ",metamodifiedon as metamodifiedon" +
                    ",sourceref as sourceref" +
                    ",orderno as orderno" +
                    ",versiontype as versiontype" +
                    " from " +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_product_series_part" +
                    " where sourceref in ('%s') and" +
                    " inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_product_series_part)\n" +
                    " order by sourceref,orderno desc\n";


    public static String getHistoryTableDataFor_stg_current_text_series =
            "select metadeleted as metadeleted" +
                    ",metamodifiedon as metamodifiedon" +
                    ",sourceref as sourceref" +
                    ",tab as tab" +
                    ",texttype as texttype" +
                    ",text as text" +
                    ",status as status" +
                    ",name as name" +
                    " from "    +
                    " bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_text_series_part" +
                    " where sourceref in ('%s') and " +
                    " inbound_ts=(select max(inbound_ts) from bcs_ingestion_database_"+getBCSDataBase.getBCSDataBase()+".stg_history_text_series_part)\n" +
                    " order by sourceref,name desc";







}
