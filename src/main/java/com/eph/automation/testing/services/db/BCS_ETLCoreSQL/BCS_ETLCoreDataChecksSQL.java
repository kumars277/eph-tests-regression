package com.eph.automation.testing.services.db.BCS_ETLCoreSQL;




public class BCS_ETLCoreDataChecksSQL {

    public static String GET_RANDOM_ACCPROD_KEY_INBOUND =
           "SELECT u_key as id \n" +
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


    public static String GET_ACCPROD_KEY_INBOUND_DATA =
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
                    "WHERE ((A.sourceref IS NOT NULL) AND (A.accountableparent IS NOT NULL)) and u_key in ('%s') order by u_key desc";


    public static String GET_ACCPROD_KEY_CURR_DATA =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_current_v \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

    public static String GET_RANDOM_MANIF_KEY_INBOUND =
            "select sourceref as id from( \n" +
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
                    "SELECT DISTINCT \"product\".\"sourceref\", \"content\".\"title\", (CASE WHEN (\"intedition\".\"classificationcode\" IS NULL) \n" +
                    "THEN false ELSE true END) \"intereditionflag\", cast((date_parse(COALESCE(NULLIF(\"firstactual\",''),\n" +
                    "NULLIF(\"firstplanned\",'')),'%d-%b-%Y')) as date ) \"firstpublisheddate\", \"product\".\"binding\", \n" +
                    "\"manifestationtypecode\".\"ephcode\" \"manifestation_type\", \"manifestationstatus\".\"ephmanifestationcode\" \"status\"\n" +
                    "   , \"workprod\".\"workmasterprojectno\" \"work_id\", CAST(NULL AS timestamp) \"last_pub_date\", 'N' \"dq_err\"\n" +
                    "   from ((((((("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_product product\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_content content ON (\"product\".\"sourceref\" = \"content\".\"sourceref\"))\n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_versionfamily workprod ON ((\"product\".\"sourceref\" = \"workprod\".\"sourceref\") AND (\"workprod\".\"workmasterprojectno\" IS NOT NULL)))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT \"sourceref\", \"classificationcode\"\n" +
                    "      FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_classification classification\n" +
                    "      WHERE (\"classificationcode\" LIKE 'PARELIE%')\n" +
                    "   ) intedition ON (\"product\".\"sourceref\" = \"intedition\".\"sourceref\"))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT \"sourceref\", \"min\"(\"plannedpubdate\") \"firstplanned\"\n" +
                    "      FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation\n" +
                    "      WHERE (\"plannedpubdate\" <> '') GROUP BY \"sourceref\"\n" +
                    "   ) planneddates ON (\"product\".\"sourceref\" = \"planneddates\".\"sourceref\"))\n" +
                    "   LEFT JOIN (\n" +
                    "      select \"sourceref\", \"min\"(\"pubdateactual\") \"firstactual\"\n" +
                    "      FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation\n" +
                    "      WHERE (\"pubdateactual\" <> '') GROUP BY \"sourceref\"\n" +
                    "   )  actualdates ON (\"product\".\"sourceref\" = \"actualdates\".\"sourceref\"))\n" +
                    "   LEFT JOIN (\n" +
                    "      SELECT distinct \"sourceref\", \"ephmanifestationcode\"\n" +
                    "      FROM ((SELECT \"sourceref\", \"min\"(\"priority\") \"statuspriority\"\n" +
                    "         FROM ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_sublocation sublocation\n" +
                    "         INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (\"split_part\"(\"status\", ' | ', 1) = \"ppmcode\"))\n" +
                    "         GROUP BY \"sourceref\")  masterstatus\n" +
                    "      INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".statuscode ON (\"statuspriority\" = \"priority\"))\n" +
                    "   )  manifestationstatus ON (\"product\".\"sourceref\" = \"manifestationstatus\".\"sourceref\"))\n" +
                    "   LEFT JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".manifestationtypecode ON (\"split_part\"(\"product\".\"versiontype\", ' | ', 1) = \"manifestationtypecode\".\"ppmcode\"))\n" +
                    ") where sourceref in ('%s') order by sourceref desc";

    public static String GET_MANIF_CURR_DATA =
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
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_manifestation_current_v where sourceref in ('%s') order by sourceref desc";

    public static String GET_RANDOM_PERSON_KEY_INBOUND =
            "SELECT sourceref as id \n" +
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
                ",dq_err as DQERR " +
                "FROM\n" +
                "  (\n" +
                "   SELECT DISTINCT\n" +
                "     businesspartnerid sourceref \n" +
                "   , (CASE WHEN (isperson = 'N') THEN NULLIF(department, '') ELSE NULLIF(firstname, '') END) firstname \n" +
                "   , (CASE WHEN (isperson = 'N') THEN NULLIF(institution, '') ELSE NULLIF(\"lastname\", '') END) familyname \n" +
                "   , CAST(null AS varchar) peoplehub_id \n" +
                "   , CAST(null AS varchar) email_address \n" +
                "   , 'N' dq_err \n" +
                "   FROM "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_originators \n" +
                ")  A \n" +
                "WHERE (A.sourceref IS NOT NULL) and sourceref in (%s) order by sourceref desc;";

    public static String GET_PERSON_CURR_DATA =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQERR " +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_current_v where sourceref in \n" +
                    "(%s) order by sourceref desc";

    public static String GET_RANDOM_WRK_RELT_KEY_INBOUND =
            "select ukey as id from( \n" +
                    "SELECT \n" +
                    "     concat(concat(CAST(sourceref AS varchar), split_part(relationtype, ' | ', 1)), CAST(projectno AS varchar)) sourceref \n" +
                    "   , sourceref parentref \n" +
                    "   , projectno childref \n" +
                    "   , relationtypecode.ephcode relationtyperef \n" +
                    "   , date_parse(metamodifiedon,`'%d-%b-%Y %H:%i:%s') modifiedon \n" +
                    "   , 'N' dq_err \n" +
                    "   FROM \n" +
                    "     ("+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".stg_current_relations \n" +
                    "   INNER JOIN "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".relationtypecode ON \n" +
                    "   (split_part(relationtype, ' | ', 1) = relationtypecode.ppmcode))) order by rand() limit %s";

}






