//created by Nishant @ 29 Sep 2020

package com.eph.automation.testing.services.db.BCSDataLakeSQL;

public class BCSDataLakeDataCheckSQL {

    public static String randomId_ingestTableFor_stg_current_classification=
            "select sourceref from(SELECT cl.value, productprojectno sourceref\n" +
                    "FROM (bcs_ingestion_database_sit.initial_ingest f CROSS JOIN UNNEST(distributionclassification) x (cl)))\n" +
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
                    "FROM  (bcs_ingestion_database_sit.initial_ingest f\n" +
                    "CROSS JOIN UNNEST(distributionclassification) x (cl)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,value,classificationcode desc";

    public static String getCurrentTableDataFor_stg_current_classification=
            "select metadeleted, metamodifiedon,sourceref,classificationcode,\n" +
                    "value,classificationtype,priority,businessunit\n" +
                    "from bcs_ingestion_database_sit.stg_current_classification \n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,value,classificationcode desc";


    //created by Nishant @ 21 Oct 2020
    public static String randomId_ingestTableFor_stg_current_content=
            "select sourceref from(SELECT df.productprojectno sourceref FROM\n" +
                    "bcs_ingestion_database_sit.initial_ingest df)  order by rand() limit %s";


    public static String getInitialIngestDataFor_stg_current_content=
            "select* from(SELECT\n" +
                    "  df.metainfdeleted\t\"metadeleted\"\n" +
                    ", df.metainfmodifiedon\t\"metamodifiedon\"\n" +
                    ", df.productprojectno\t\"sourceref\"\n" +
                    ", df.contentoriginimpid\t\"originimpid\"\n" +
                    ", df.contentsubgroup\t\"subgroup\"\n" +
                    ", df.contentseries\t\"series\"\n" +
                    ", df.contentcopyrightyear\t\"copyrightyear\"\n" +
                    ", df.contenttitle\t\"title\"\n" +
                    ", df.contentworktitle\t\"worktitle\"\n" +
                    ", df.contentisset\t\"isset\"\n" +
                    "--, df.contentimpressionid\t\"impressionid\"\n" +
                    ", df.contentdoistatus\t\"doistatus\"\n" +
                    ", df.contentimprint\t\"imprint\"\n" +
                    ", df.contentdivision\t\"division\"\n" +
                    ", df.contentlanguage3\t\"language3\"\n" +
                    ", df.contentregstatus\t\"regstatus\"\n" +
                    ", df.contentapprovedondate\t\"approvedondate\"\n" +
                    ", df.contentlanguage2\t\"language2\"\n" +
                    ", df.contentdoi\t\"doi\"\n" +
                    ", df.contentvolumeno\t\"volumeno\"\n" +
                    ", df.contenteditionid\t\"editionid\"\n" +
                    ", df.contentsynctemplate\t\"synctemplate\"\n" +
                    ", df.contentvolumename\t\"volumename\"\n" +
                    ", df.contentseriesissn\t\"seriesissn\"\n" +
                    ", df.contentseriesid\t\"seriesid\"\n" +
                    ", df.contentownership\t\"ownership\"\n" +
                    ", df.contentfirstapproval\t\"firstapproval\"\n" +
                    ", df.contentcompanygroup\t\"companygroup\"\n" +
                    ", df.contentshorttitle\t\"shorttitle\"\n" +
                    ", df.contenteditionno\t\"editionno\"\n" +
                    ", df.contentwmyn\t\"work_master_flag\"\n" +
                    ", df.contentlanguage\t\"language\"\n" +
                    ", df.contentpiidack\t\"piidack\"\n" +
                    ", df.contentpublisher\t\"publisher\"\n" +
                    ", df.contenttitleid\t\"titleid\"\n" +
                    ", df.contentsubtitle\t\"subtitle\"\n" +
                    ", df.contentseriescode\t\"seriescode\"\n" +
                    ", df.contentobjtype\t\"objecttype\"\n" +
                    "FROM bcs_ingestion_database_sit.initial_ingest df) \n" +
                    "  where sourceref in ('%s') order by sourceref desc";

    public static String getCurrentTableDataFor_stg_current_content=
            "select metadeleted,metamodifiedon,sourceref,originimpid,subgroup,series,copyrightyear,title,worktitle," +
                    "isset,doistatus,imprint,division,language3,regstatus,approvedondate,language2,doi," +
                    "volumeno,editionid,synctemplate,volumename,seriesissn,seriesid,ownership,firstapproval,companygroup," +
                    "shorttitle,editionno,work_master_flag,language,piidack,publisher,titleid,subtitle,seriescode,objecttype " +
                    "from bcs_ingestion_database_sit.stg_current_content \n" +
                    "where sourceref in ('%s') order by sourceref desc";

    //added by Nishant @ 22 Oct 2020
    public static String randomId_ingestTableFor_stg_current_extobject=
            "select sourceref from(select productprojectno sourceref, cj.object, cj.name\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(\"productexternalobjects\") x (cj)))\n" +
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
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(productexternalobjects) x (cj)))\n" +
                    "where sourceref in ('%s') \n" +
                    "order by sourceref,name,object desc";

    public static String getCurrentTableDataFor_stg_current_extobject=
            "select metadeleted,metamodifiedon,sourceref,object,type,name,comments,source \n" +
                    "from bcs_ingestion_database_sit.stg_current_extobject \n" +
                    "where sourceref in ('%s') order by sourceref,name,object desc";


    public static String randomId_ingestTableFor_stg_current_fullversionfamily=
            "select sourceref from (select productprojectno sourceref, fv.projectno\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
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
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(contentfullversionfamily) x (fv)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,projectno desc";

    public static String getCurrentTableDataFor_stg_current_fullversionfamily=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "versiontype,editionno,isbn,projectno,workmaster\n" +
                    "from bcs_ingestion_database_sit.stg_current_fullversionfamily \n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,projectno desc";


    public static String randomId_ingestTableFor_stg_current_originatoraddress=
            "with originator as (select\n" +
                    "    df.metainfdeleted metadeleted\n" +
                    "  , df.metainfmodifiedon metamodifiedon\n" +
                    "  --, df.productprojectno sourceref\n" +
                    "  , co.businesspartnerid businesspartnerid\n" +
                    "  , co.authoraddress\n" +
                    "  from (bcs_ingestion_database_sit.initial_ingest df\n" +
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
                    "  from (bcs_ingestion_database_sit.initial_ingest df\n" +
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
                    "mobile,district from bcs_ingestion_database_sit.stg_current_originatoraddress \n" +
                    "where businesspartnerid in (%s)\n" +
                    "order by businesspartnerid,country,metamodifiedon desc";



    public static String randomId_ingestTableFor_stg_current_originators=
            "select sourceref from " +
                    "(select df.productprojectno sourceref, co.businesspartnerid businesspartnerid\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df CROSS JOIN UNNEST(contactsoriginators) x (co)))\n" +
                    "where businesspartnerid is not null order by rand() limit %s";

    public static String getInitialIngestDataFor_stg_current_originators=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", co.prefix\tprefix\n" +
                    ", co.sequence\tsequence\n" +
                    ", co.businesspartnerid\tbusinesspartnerid\n" +
                    ", co.originatortitle\toriginatorid\n" +
                    ", co.isperson\tisperson\n" +
                    ", co.locationid\tlocationid\n" +
                    ", co.copyrightholdertype\tcopyrightholdertype\n" +
                    ", co.institution\tinstitution\n" +
                    ", co.firstname\tfirstname\n" +
                    ", co.department\tdepartment\n" +
                    ", co.lastname\tlastname\n" +
                    ", co.searchterm\tsearchterm\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(contactsoriginators) x (co)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,copyrightholdertype,businesspartnerid desc";

    public static String getCurrentTableDataFor_stg_current_originators=
            "select metadeleted,metamodifiedon,sourceref, prefix,sequence,businesspartnerid,originatorid,\n" +
                    "isperson,locationid,copyrightholdertype,institution,\n" +
                    "firstname,department,lastname,searchterm\n" +
                    "from bcs_ingestion_database_sit.stg_current_originators \n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,copyrightholdertype,businesspartnerid desc";

    public static String randomId_ingestTableFor_stg_current_pricing=
            "select sourceref from(select \n" +
                    " df.productprojectno sourceref\n" +
                    ", cp.validupto validto\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
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
                    "from (bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(productprice) x (cp)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,type,currency,validto desc";
    public static String getCurrentTableDataFor_stg_current_pricing=
            "select metadeleted,metamodifiedon,sourceref,\n" +
                    "validfrom,type,currency,priceapprox,price,validto\n" +
                    "from bcs_ingestion_database_sit.stg_current_pricing\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,type,currency,validto desc";

    public static String randomId_ingestTableFor_stg_current_product=
            "select sourceref from(select df.productprojectno sourceref\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df)) \n" +
                    "order by rand() limit %s";
    public static String getInitialIngestDataFor_stg_current_product=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", df.productnoofvolumes\tnoofvolumes\n" +
                    ", df.productrefkey\trefkey\n" +
                    ", df.productexternaleditionid\texternaleditionid\n" +
                    ", df.productisbn13\tisbn13\n" +
                    ", df.productversiontype\tversiontype\n" +
                    ", df.productpodsuitable\tpodsuitable\n" +
                    ", df.productfirstrelease\tfirstrelease\n" +
                    ", df.productorigtitle\torigtitle\n" +
                    ", df.productinventoryukavailablestock\tukavailablestock\n" +
                    ", df.productinventorygertotalstock\tgertotalstock\n" +
                    ", df.productinventoryfraavailablestock\tfraavailablestock\n" +
                    ", df.productinventoryausavailablestock\tausavailablestock\n" +
                    ", df.productinventoryustotalstock\tustotalstock\n" +
                    ", df.productinventoryuktotalstock\tuktotalstock\n" +
                    ", df.productinventorygeravailablestock\tgeravailablestock\n" +
                    ", df.productinventoryfratotalstock\tfratotalstock\n" +
                    ", df.productinventoryusavailablestock\tusavailablestock\n" +
                    ", df.productplanned\tplanned\n" +
                    ", df.productprojectno\tprojectno\n" +
                    ", df.productisbn\tisbn\n" +
                    ", df.productcontractpubdate\tcontractpubdate\n" +
                    ", df.productmodifiedon\tmodifiedon\n" +
                    ", df.productunitcost\tunitcost\n" +
                    ", df.productpublishedon\tpublishedon\n" +
                    ", df.productdeliverystatus\tdeliverystatus\n" +
                    ", df.productplannededitionsize\tplannededitionsize\n" +
                    ", df.productlatestpubdate\tlatestpubdate\n" +
                    ", df.productmedium\tmedium\n" +
                    ", df.productorderno\torderno\n" +
                    ", df.productfirstprinting\tfirstprinting\n" +
                    ", df.productreason\treason\n" +
                    ", df.productbudgetpubdate\tbudgetpubdate\n" +
                    ", df.productpubdateplanned\tpubdateplanned\n" +
                    ", df.productcreatedon\tcreatedon\n" +
                    "--, df.productexternalimpressionid\texternalimpressionid\n" +
                    ", df.productplannedfirstprint\tplannedfirstprint\n" +
                    ", df.productbinding\tbinding\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df))\n" +
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
                    "from bcs_ingestion_database_sit.stg_current_product \n" +
                    "where sourceref in('%s')\n" +
                    "order by sourceref desc";

    public static String randomId_ingestTableFor_stg_current_production=
            "select sourceref from(select df.productprojectno sourceref\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df)) \n" +
                    "order by rand() limit %s";
    public static String getInitialIngestDataFor_stg_current_production=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", df.productionweight\tweight\n" +
                    ", df.productionlineartbw\tlineartbw\n" +
                    ", df.coverendpaperfrontpapercolour\tfrontpapercolour\n" +
                    ", df.coverendpaperbackpapercolour\tbackpapercolour\n" +
                    ", df.coverendpapergrammage\tgrammage\n" +
                    ", df.coverendpaperbackcoverpms\tbackcoverpms\n" +
                    ", df.coverendpaperfrontcoverpms\tfrontcoverpms\n" +
                    ", df.productionapproxpages\tapproxpages\n" +
                    "--, df.productionreprintno\treprintno\n" +
                    ", df.productionlineartcolors\tlineartcolors\n" +
                    ", df.productionsupplierb\tsupplierb\n" +
                    ", df.productiontablesbw\ttablesbw\n" +
                    ", df.productionauthoringsystem\tauthoringsystem\n" +
                    ", df.productiontagging\ttagging\n" +
                    ", df.productionsectioncolours\tsectioncolours\n" +
                    ", df.productionaddillustration\taddillustration\n" +
                    ", df.productionpagesroman\tpagesroman\n" +
                    ", df.productionproductionmethod\tproductionmethod\n" +
                    ", df.productionsupplierashortname\tsupplierashortname\n" +
                    ", df.productionsupplierafullname\tsupplierafullname\n" +
                    ", df.productionbindmeth\tbindmeth\n" +
                    ", df.productionformat\tformat\n" +
                    ", df.productiontrimsize\ttrimsize\n" +
                    ", df.productiontrimother\ttrimother\n" +
                    ", df.productionmstype\tmstype\n" +
                    ", df.productionmanuscriptpages\tmanuscriptpages\n" +
                    ", df.productiontablescolors\ttablescolors\n" +
                    ", df.productionsupplierbfullname\tsupplierbfullname\n" +
                    ", df.productionproductiondetails\tproductiondetails\n" +
                    ", df.productionillustrationscolors\tillustrationscolors\n" +
                    ", df.productiongraphicscolors\tgraphicscolors\n" +
                    ", df.productionexternalads\texternalads\n" +
                    ", df.productioninternalads\tinternalads\n" +
                    ", df.productionhalftonesbw\thalftonesbw\n" +
                    ", df.productionmapsbw\tmapsbw\n" +
                    ", df.productionspinestyle\tspinestyle\n" +
                    ", df.productioneonlypages\teonlypages\n" +
                    ", df.productionsuppliera\tsuppliera\n" +
                    ", df.productionsupplierbshortname\tsupplierbshortname\n" +
                    ", df.productionillustrationsbw\tillustrationsbw\n" +
                    ", df.productionextentprod\textentprod\n" +
                    ", df.productionprintform\tprintform\n" +
                    ", df.productionduotone\tduotone\n" +
                    ", df.productioncopyedlevel\tcopyedlevel\n" +
                    ", df.coverbindingexteriorpms\texteriorpms\n" +
                    ", df.coverbindingfinishing\tfinishing\n" +
                    ", df.coverbindingmaterial\tmaterial\n" +
                    ", df.coverbindingpaperquality\tpaperquality\n" +
                    ", df.coverbindinginteriorpms\tinteriorpms\n" +
                    "--, df.coverbindinggrammage\tgrammage\n" +
                    ", df.coverbindinginteriorcolour\tinteriorcolour\n" +
                    ", df.coverbindingboardthickness\tboardthickness\n" +
                    ", df.coverbindingspec\tspec\n" +
                    ", df.productionmapscolor\tmapscolor\n" +
                    ", df.productiontextdesigntype\ttextdesigntype\n" +
                    ", df.productionclassification\tclassification\n" +
                    ", df.productionhalftonescolors\thalftonescolors\n" +
                    ", df.productionbiblioreference\tbiblioreference\n" +
                    ", df.productionextentstatus\textentstatus\n" +
                    ", df.productionpagesarabic\tpagesarabic\n" +
                    ", df.productiongraphicsbw\tgraphicsbw\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df))\n" +
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
                    "from bcs_ingestion_database_sit.stg_current_production \n" +
                    "where sourceref in ('%s')order by sourceref desc";

    public static String randomId_ingestTableFor_stg_current_relations=
            "select sourceref from(select df.productprojectno sourceref, cj.projectno projectno\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df      \n" +
                    "CROSS JOIN UNNEST(productrelation) x (cj))) \n" +
                    "where projectno is not null\n" +
                    "order by rand() limit %s";
    public static String getInitialIngestDataFor_stg_current_relations=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", cj.orderno orderno\n" +
                    ", cj.relationtype\trelationtype\n" +
                    ", cj.projectno projectno\n" +
                    "--, cj.relimpressionid relimpressionid\n" +
                    ", cj.isbn isbn\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df \n" +
                    "CROSS JOIN UNNEST(productrelation) x (cj)))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,relationtype,projectno desc";
    public static String getCurrentTableDataFor_stg_current_relations=
            "select metadeleted,metamodifiedon,sourceref,orderno\n" +
                    ",relationtype,projectno--,relimpressionid\n" +
                    ",isbn\n" +
                    "from bcs_ingestion_database_sit.stg_current_relations\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,relationtype,projectno desc";

    public static String randomId_ingestTableFor_stg_current_responsibilities=
            "select sourceref from(select \n" +
                    " df.productprojectno sourceref, cj.responsibleperson\tresponsibleperson\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df \n" +
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
                    "from (bcs_ingestion_database_sit.initial_ingest df \n" +
                    "CROSS JOIN UNNEST(contactsresponsibilities) x (cj)))\n" +
                    "where sourceref in ('%s')\n" +
                    " order by  sourceref,responsibility,responsibleperson desc";
    public static String getCurrentTableDataFor_stg_current_responsibilities=
            "select metadeleted,metamodifiedon,sourceref,responsibility\n" +
                    ",responsibleperson,personid\n" +
                    "from bcs_ingestion_database_sit.stg_current_responsibilities \n" +
                    "where sourceref in ('%s')\n" +
                    "order by  sourceref,responsibility,responsibleperson desc";

    public static String randomId_ingestTableFor_stg_current_sublocation=
            "select sourceref from(select \n" +
                    " df.productprojectno sourceref\n" +
                    ", cj.warehouse\twarehouse\n" +
                    "from (bcs_ingestion_database_sit.initial_ingest df \n" +
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
                    "from (bcs_ingestion_database_sit.initial_ingest df \n" +
                    "CROSS JOIN UNNEST(productsublocation) x (cj)))\n" +
                    "where sourceref in('%s')\n" +
                    "order by sourceref,warehouse desc";

    public static String getCurrentTableDataFor_stg_current_sublocation=
            "select metadeleted,metamodifiedon,sourceref,warehouse\n" +
                    ",pubdateactual,stocksplit, refkey,status,plannedpubdate\n" +
                    "from bcs_ingestion_database_sit.stg_current_sublocation\n" +
                    "where sourceref in('%s')\n" +
                    "order by sourceref,warehouse desc";


    public static String randomId_ingestTableFor_stg_current_text=
            "select sourceref from(select \n" +
                    " df.productprojectno sourceref, cj.text\n" +
                    "from bcs_ingestion_database_sit.initial_ingest df\n" +
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
                    "from bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(distributiontext) x (cj))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,texttype,text desc";
    public static String getCurrentTableDataFor_stg_current_text=
            "select metadeleted,metamodifiedon,sourceref,tab\n" +
                    ",texttype,name, text,status\n" +
                    "from bcs_ingestion_database_sit.stg_current_text\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,texttype,text desc";

    public static String randomId_ingestTableFor_stg_current_versionfamily=
            "select sourceref from(select \n" +
                    " df.productprojectno sourceref\n" +
                    ", cj.childprojectno childprojectno\n" +
                    "from bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(contentversionfamily) x (cj))\n" +
                    "where childprojectno is not null and childprojectno!=''\n" +
                    "order by rand() limit %s";
    public static String getInitialIngestDataFor_stg_current_versionfamily=
            "select * from(select \n" +
                    "  df.metainfdeleted metadeleted\n" +
                    ", df.metainfmodifiedon metamodifiedon\n" +
                    ", df.productprojectno sourceref\n" +
                    ", cj.workmasterisbn workmasterisbn\n" +
                    ", cj.workmasterprojectno workmasterprojectno\n" +
                    ", cj.childisbn childisbn\n" +
                    ", cj.childprojectno childprojectno\n" +
                    "from bcs_ingestion_database_sit.initial_ingest df\n" +
                    "CROSS JOIN UNNEST(contentversionfamily) x (cj))\n" +
                    "where sourceref in ('%s')\n" +
                    "order by sourceref,childprojectno desc";
    public static String getCurrentTableDataFor_stg_current_versionfamily=
            "select metadeleted,metamodifiedon,sourceref,workmasterisbn\n" +
                    ",workmasterprojectno,childisbn,childprojectno\n" +
                    "from bcs_ingestion_database_sit.stg_current_versionfamily\n" +
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
                    */


}
