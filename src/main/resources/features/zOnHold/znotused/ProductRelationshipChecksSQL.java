package features.zOnHold.znotused;

import com.eph.automation.testing.services.db.sql.GetEPHDBUser;

/**
 * Created by Bistra Drazheva on 20/02/2019
 */
public class ProductRelationshipChecksSQL {

    public static String GET_PMX_PRODUCT_RELATIONSHIPS_COUNT = "SELECT count(*) as count FROM (SELECT\n" +
            "\t WL.PRODUCT_WORK_LINK_ID AS RELATIONSHIP_PMX_SOURCEREF\n" +
            "\t,W1.PRODUCT_WORK_ID || '-PKG' AS OWNER_PMX_SOURCE\n" +
            "\t,M2.PRODUCT_MANIFESTATION_ID || '-SUB' AS COMPONENT_PMX_SOURCE\n" +
            "\t,'CON' AS F_RELATIONSHIP_TYPE\n" +
            "\t,CASE WHEN W1.F_WORK_STATUS = 81 AND EFFFROM_DATE IS NULL THEN TO_DATE('2019-01-01', 'YYYY-MM-DD')\n" +
            "\t      WHEN W1.F_WORK_STATUS = 81 AND EFFFROM_DATE IS NOT NULL THEN EFFFROM_DATE\n" +
            "\t      ELSE NULL END AS EFFECTIVE_START_DATE  -- available work (81)\n" +
            "\t,NVL(WL.EFFTO_DATE,\n" +
            "\t\tCASE WHEN W1.EFFECTIVE_TO_DATE IS NULL AND W2.EFFECTIVE_TO_DATE IS NULL THEN NULL \n" +
            "\t\tELSE GREATEST(NVL(W1.EFFECTIVE_TO_DATE,TO_DATE('1900-01-01', 'YYYY-MM-DD')),NVL(W2.EFFECTIVE_TO_DATE,TO_DATE('1900-01-01', 'YYYY-MM-DD'))) END) AS ENDON\n" +
            "\t,TO_CHAR(NVL(WL.B_UPDDATE,WL.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM\n" +
            "\tGD_PRODUCT_WORK_LINK WL,\n" +
            "\tGD_PRODUCT_WORK W1,\n" +
            "\tGD_PRODUCT_WORK W2,\n" +
            "\tGD_PRODUCT_MANIFESTATION M2\n" +
            "WHERE\n" +
            "\tWL.F_PRODUCT_WORK = W1.PRODUCT_WORK_ID\n" +
            "AND\n" +
            "\tWL.F_RELATED_PRODUCT_WORK = W2.PRODUCT_WORK_ID\n" +
            "AND\n" +
            "\tW2.PRODUCT_WORK_ID = M2.F_PRODUCT_WORK\n" +
            "AND\n" +
            "\tW1.F_PRODUCT_TYPE IN (21,143)\t\t-- full sets & subject collections\n" +
            "AND\n" +
            "\tW2.F_PRODUCT_TYPE = 4\t\t\t-- journals\n" +
            "AND\n" +
            "\tM2.F_PRODUCT_MANIFESTATION_TYP = 2  \t-- electronic\n" +
            "AND\n" +
            "\tW1.F_WORK_STATUS = 81\n" +
            "AND\n" +
            "\tWL.F_PRODUCT_WORK_LINK_TYPE = 42\t-- includes\n" +
            "\t)\n";

    public static String GET_EPH_STG_PRODUCT_RELATIONSHIPS_COUNT = "select count(*) as count from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_pack_rel\n";

    public static String GET_EPH_STG_PRODUCT_RELATIONSHIPS_COUNT_GOING_TO_SA = "select  count(*) as count \n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_pack_rel\n" +
            "join (\n" +
            "select s.pmx_source_reference as stage,\n" +
            "g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_product g \n" +
            "full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s on g.external_reference = s.pmx_source_reference::varchar) d1 \n" +
            "on STG_10_PMX_PRODUCT_PACK_REL.\"OWNER_PMX_SOURCE\" = d1.consol\n" +
            "join (select s.pmx_source_reference as stage,\n" +
            "g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_product g \n" +
            "full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s \n" +
            "on g.external_reference = s.pmx_source_reference::varchar) d2\n" +
            "on STG_10_PMX_PRODUCT_PACK_REL.\"COMPONENT_PMX_SOURCE\" = d2.consol where d1.dq_err!= 'Y' and d2.dq_err!= 'Y'\t\n";


    public static String GET_EPH_STG_PRODUCT_RELATIONSHIPS_COUNT_DELTA = "select  count(*) as count \n" +
            "from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_pack_rel\n" +
            "join (\n" +
            "select s.pmx_source_reference as stage,\n" +
            "g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_product g \n" +
            "full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s on g.external_reference = s.pmx_source_reference::varchar) d1 \n" +
            "on STG_10_PMX_PRODUCT_PACK_REL.\"OWNER_PMX_SOURCE\" = d1.consol\n" +
            "join (select s.pmx_source_reference as stage,\n" +
            "g.external_reference as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err\n" +
            "from semarchy_eph_mdm.gd_product g \n" +
            "full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s \n" +
            "on g.external_reference = s.pmx_source_reference::varchar) d2\n" +
            "on STG_10_PMX_PRODUCT_PACK_REL.\"COMPONENT_PMX_SOURCE\" = d2.consol where d1.dq_err!= 'Y' and d2.dq_err!= 'Y'\n" +
            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('%s','YYYYMMDDHH24MI')";
//            "and TO_TIMESTAMP(\"UPDATED\",'YYYYMMDDHH24MI') > TO_TIMESTAMP('201905201200','YYYYMMDDHH24MI')";


//  old  public static String GET_EPH_SA_PRODUCT_RELATIONSHIPS_COUNT = "select count(*) as count from semarchy_eph_mdm.sa_product_rel_package\n" +
//            "where f_event = (select max (f_event) from semarchy_eph_mdm.sa_product_rel_package\n" +
//            "join semarchy_eph_mdm.sa_event on f_event = event_id \n" +
//            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
//            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
//            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
//            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')";

    public static String GET_EPH_SA_PRODUCT_RELATIONSHIPS_COUNT = "select count(*) as count from semarchy_eph_mdm.sa_product_rel_package\n" +
            "where f_event = (select  max(event_id) from semarchy_eph_mdm.sa_event sa2  \n" +
            "where sa2.f_event_type = 'PMX'\n" +
            "and sa2.workflow_id = 'talend'\n" +
            "and sa2.f_workflow_source = 'PMX')";

    public static String GET_EPH_GD_PRODUCT_RELATIONSHIPS_COUNT = "select count(*) as count from semarchy_eph_mdm.gd_product_rel_package where b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX' ) ";

    public static String GET_COUNT_PRODUCT_RELATIONSHIP_EPHAE = "select count(*) as count from semarchy_eph_mdm.ae_product_rel_package where b_batchid = (select max (b_batchid) from \n" +
            "          semarchy_eph_mdm.gd_event\n" +
            "            where  f_event_type = 'PMX'\n" +
            "            and workflow_id = 'talend'\n" +
            "            AND f_event_type = 'PMX'\n" +
            "            and f_workflow_source = 'PMX' )";

    public static String GET_PMX_PRODUCT_RELATIONSHIPS_DATA = "SELECT * FROM (SELECT\n" +
            "\t WL.PRODUCT_WORK_LINK_ID || M2.PRODUCT_MANIFESTATION_ID || '-SUB' AS RELATIONSHIP_PMX_SOURCEREF\n" +
            "\t,W1.PRODUCT_WORK_ID || '-PKG' AS OWNER_PMX_SOURCE\n" +
            "\t,M2.PRODUCT_MANIFESTATION_ID || '-SUB' AS COMPONENT_PMX_SOURCE\n" +
            "\t,'CON' AS F_RELATIONSHIP_TYPE\n" +
            "\t,CASE WHEN W1.F_WORK_STATUS = 81 AND EFFFROM_DATE IS NULL THEN TO_DATE('2019-01-01', 'YYYY-MM-DD')\n" +
            "\t      WHEN W1.F_WORK_STATUS = 81 AND EFFFROM_DATE IS NOT NULL THEN EFFFROM_DATE\n" +
            "\t      ELSE NULL END AS EFFECTIVE_START_DATE  -- available work (81)\n" +
            "\t,NVL(WL.EFFTO_DATE,\n" +
            "\t\tCASE WHEN W1.EFFECTIVE_TO_DATE IS NULL AND W2.EFFECTIVE_TO_DATE IS NULL THEN NULL \n" +
            "\t\tELSE GREATEST(NVL(W1.EFFECTIVE_TO_DATE,TO_DATE('1900-01-01', 'YYYY-MM-DD')),NVL(W2.EFFECTIVE_TO_DATE,TO_DATE('1900-01-01', 'YYYY-MM-DD'))) END) AS ENDON\n" +
            "\t,TO_CHAR(NVL(WL.B_UPDDATE,WL.B_CREDATE),'YYYYMMDDHH24MI') AS UPDATED\n" +
            "FROM\n" +
            "\tGD_PRODUCT_WORK_LINK WL,\n" +
            "\tGD_PRODUCT_WORK W1,\n" +
            "\tGD_PRODUCT_WORK W2,\n" +
            "\tGD_PRODUCT_MANIFESTATION M2\n" +
            "WHERE\n" +
            "\tWL.F_PRODUCT_WORK = W1.PRODUCT_WORK_ID\n" +
            "AND\n" +
            "\tWL.F_RELATED_PRODUCT_WORK = W2.PRODUCT_WORK_ID\n" +
            "AND\n" +
            "\tW2.PRODUCT_WORK_ID = M2.F_PRODUCT_WORK\n" +
            "AND\n" +
            "\tW1.F_PRODUCT_TYPE IN (21,143)\t\t-- full sets & subject collections\n" +
            "AND\n" +
            "\tW2.F_PRODUCT_TYPE = 4\t\t\t-- journals\n" +
            "AND\n" +
            "\tM2.F_PRODUCT_MANIFESTATION_TYP = 2  \t-- electronic\n" +
            "AND\n" +
            "\tW1.F_WORK_STATUS = 81\n" +
            "AND\n" +
            "\tWL.F_PRODUCT_WORK_LINK_TYPE = 42\t-- includes\n" +
            "\t) where RELATIONSHIP_PMX_SOURCEREF in ('%s') order by RELATIONSHIP_PMX_SOURCEREF desc";

    public static String GET_EPH_STG_PRODUCT_RELATIONSHIPS_DATA = "select\n" +
            "   \"RELATIONSHIP_PMX_SOURCEREF\" as RELATIONSHIP_PMX_SOURCEREF,\n" +
            "   \"OWNER_PMX_SOURCE\" as OWNER_PMX_SOURCE,\n" +
            "   \"COMPONENT_PMX_SOURCE\" as COMPONENT_PMX_SOURCE,\n" +
            "   \"F_RELATIONSHIP_TYPE\" as F_RELATIONSHIP_TYPE,\n" +
            "   \"EFFECTIVE_START_DATE\" as EFFECTIVE_START_DATE,\n" +
            "   \"ENDON\" as ENDON,\n" +
            "   \"UPDATED\" as UPDATED, \n" +
            "   d1.status as status1,\n" +
            "   d2.status as status2" +
            "   from " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_pack_rel\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference  as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err,\n" +
            "coalesce(s.f_status,g.f_status) as status\n" +
            "from semarchy_eph_mdm.gd_product g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s on g.external_reference = s.pmx_source_reference::varchar) d1\n" +
            "on STG_10_PMX_PRODUCT_PACK_REL.\"OWNER_PMX_SOURCE\" = d1.consol\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference  as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err,\n" +
            "coalesce(s.f_status,g.f_status) as status\n" +
            "from semarchy_eph_mdm.gd_product g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s on g.external_reference = s.pmx_source_reference::varchar) d2\n" +
            "on STG_10_PMX_PRODUCT_PACK_REL.\"COMPONENT_PMX_SOURCE\" = d2.consol\n" +
            "left join\n" +
            "    (select distinct external_reference, product_rel_pack_id from semarchy_eph_mdm.sa_product_rel_package) a \n" +
            "    on STG_10_PMX_PRODUCT_PACK_REL.\"RELATIONSHIP_PMX_SOURCEREF\"::varchar = a.external_reference\n"+
            "   where \"RELATIONSHIP_PMX_SOURCEREF\"  in ('%s') " +
            " order by RELATIONSHIP_PMX_SOURCEREF desc";

    public static String SELECT_RANDOM_RELATIONSHIP_PMX_SOURCEREF = "select \"RELATIONSHIP_PMX_SOURCEREF\" as RELATIONSHIP_PMX_SOURCEREF from \n" +
            "" + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_pack_rel \n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference  as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err,\n" +
            "coalesce(s.f_status,g.f_status) as status\n" +
            "from semarchy_eph_mdm.gd_product g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s on g.external_reference = s.pmx_source_reference::varchar) d1\n" +
            "on STG_10_PMX_PRODUCT_PACK_REL.\"OWNER_PMX_SOURCE\" = d1.consol\n" +
            "join  (select s.pmx_source_reference as stage, g.external_reference  as gold,\n" +
            "coalesce(s.pmx_source_reference::varchar,g.external_reference) as consol,\n" +
            "case when s.pmx_source_reference is null then 'N' else s.dq_err end as dq_err,\n" +
            "coalesce(s.f_status,g.f_status) as status\n" +
            "from semarchy_eph_mdm.gd_product g full outer join " + GetEPHDBUser.getDBUser() + ".stg_10_pmx_product_dq s on g.external_reference = s.pmx_source_reference::varchar) d2\n" +
            "on STG_10_PMX_PRODUCT_PACK_REL.\"COMPONENT_PMX_SOURCE\" = d2.consol\n" +
            "left join\n" +
            "    (select distinct external_reference, product_rel_pack_id from semarchy_eph_mdm.sa_product_rel_package) a \n" +
            "    on STG_10_PMX_PRODUCT_PACK_REL.\"RELATIONSHIP_PMX_SOURCEREF\"::varchar = a.external_reference\n" +
            "order by random() limit '%s'";

    public static String GET_PRODUCT_REL_PACK_ID_FROM_LOOKUP_TABLE = "select numeric_id as PRODUCT_REL_PACK_ID\n" +
            "FROM " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_numericid where source_ref in ('%s')";

    public static String GET_RELATIONSHIP_PMX_SOURCEREF_FROM_LOOKUP_TABLE = "select numeric_id as PRODUCT_REL_PACK_ID\n" +
            "FROM " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_numericid where source_ref in ('%s')";

    public static String GET_ID_FROM_SOURCEREF_LOOKUP_TABLE = "SELECT eph_id as eph_id\n" +
            "FROM " + GetEPHDBUser.getDBUser() + ".map_sourceref_2_ephid where source_ref in ('%s')";

    public static String GET_EPH_SA_PRODUCT_RELATIONSHIPS_DATA = "select \n" +
            "sa.b_loadid as B_LOADID,\n" +
            "f_event as F_EVENT,\n" +
            "sa.b_classname as B_CLASSNAME,\n" +
            "product_rel_pack_id as PRODUCT_REL_PACK_ID,\n" +
            "f_package_owner as F_PACKAGE_OWNER,\n" +
            "f_component as F_COMPONENT,\n" +
            "f_relationship_type as F_RELATIONSHIP_TYPE,\n" +
            "effective_start_date as EFFECTIVE_START_DATE,\n" +
            "effective_end_date as EFFECTIVE_END_DATE\n" +
            "from semarchy_eph_mdm.sa_product_rel_package sa\n" +
            "where f_event = (select max (f_event) from semarchy_eph_mdm.sa_product_rel_package\n" +
            "join semarchy_eph_mdm.sa_event on f_event = event_id \n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.workflow_id = 'talend'\n" +
            "and semarchy_eph_mdm.sa_event.f_event_type = 'PMX'\n" +
            "and semarchy_eph_mdm.sa_event.f_workflow_source = 'PMX')\n" +
            "and \"external_reference\"  in ('%s')\n" +
            "order by external_reference desc";


    public static String GET_EPH_GD_PRODUCT_RELATIONSHIPS_DATA = "select \n" +
            "f_event as F_EVENT,\n" +
            "b_classname as B_CLASSNAME,\n" +
            "product_rel_pack_id as PRODUCT_REL_PACK_ID,\n" +
            "f_package_owner as F_PACKAGE_OWNER,\n" +
            "f_component as F_COMPONENT,\n" +
            "f_relationship_type as F_RELATIONSHIP_TYPE,\n" +
            "effective_start_date as EFFECTIVE_START_DATE,\n" +
            "effective_end_date as EFFECTIVE_END_DATE\n" +
            "from semarchy_eph_mdm.gd_product_rel_package\n" +
            "where \"external_reference\"  in ('%s')\n" +
            "order by external_reference desc";


}
