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
                    "WHERE ((A.sourceref IS NOT NULL) AND (A.accountableparent IS NOT NULL)) and u_key in ('%s') order by u_key desc";




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




    public static String GET_RANDOM_ACCPROD_KEY_CURRENT =
            "SELECT u_key as u_key \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_current_v \n" +
                    " order by rand() limit %s";

    public static String GET_ACCPROD_REC_CURR_DATA =
            "SELECT sourceref as SOURCEREF \n" +
                    ",accountableproduct as ACCOUNTABLEPRODUCT \n" +
                    ",accountablename as ACCOUNTABLENAME \n" +
                    ",accountableparent as ACCOUNTABLEPARENT \n" +
                    ",u_key as UKEY \n" +
                    ",dq_err as DQ_ERR \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_accountable_product_current_v \n" +
                    "where u_key in ('%s') \n" +
                    "order by u_key desc";

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


    public static String GET_RANDOM_PERSON_KEY_CURRENT =
            "SELECT u_key as u_key \n" +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_person_current_v \n" +
                    " order by rand() limit %s";

    public static String GET_PERSON_CURR_DATA =
            "select " +
                    "sourceref as SOURCEREF" +
                    ",u_key as UKEY" +
                    ",firstname as FIRSTNAME" +
                    ",familyname as FAMILYNAME" +
                    ",peoplehub_id as PEOPLEHUBID" +
                    ",email_address as EMAIL" +
                    ",dq_err as DQERR " +
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
                    ",dq_err as DQERR " +
                    "from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_part  where " +
                    "transform_ts = (select max(transform_ts) from "+GetBCS_ETLCoreDLDBUser.getBCS_ETLCoreDataBase()+".etl_transform_history_person_part)" +
                    "and u_key in (%s) \n" +
                    "order by u_key desc";









}






