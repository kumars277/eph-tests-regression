package com.eph.automation.testing.services.db.sql;

/**
 * Created by Bistra Drazheva on 20/02/2019
 */
public class ProductRelationshipChecksSQL {

    public static String GET_PMX_PRODUCT_RELATIONSHIPS_COUNT = "SELECT \n" +
            "\t count(*)\n as count\n" +
            "FROM \n" +
            "\tGD_PRODUCT_WORK_LINK WL,\n"+
            "\tGD_PRODUCT_WORK W1,\n"+
            "\tGD_PRODUCT_WORK W2,\n"+
            "\tGD_PRODUCT_MANIFESTATION M2\n"+
            "WHERE\n"+
            "\tWL.F_PRODUCT_WORK = W1.PRODUCT_WORK_ID\n"+
            "AND\t\n"+
            "\tWL.F_RELATED_PRODUCT_WORK = W2.PRODUCT_WORK_ID\n"+
            "AND\n"+
            "\tW2.PRODUCT_WORK_ID = M2.F_PRODUCT_WORK\n"+
            "AND\n"+
            "\tW1.F_PRODUCT_TYPE IN (21,143)\t\t-- full sets & subject collections\n"+
            "AND\t\n"+
            "\tW2.F_PRODUCT_TYPE = 4\t\t\t-- journals\n"+
            "AND\n"+
            "\tM2.F_PRODUCT_MANIFESTATION_TYP = 2  \t-- electronic \n"+
            "AND\n"+
            "\tWL.EFFTO_DATE IS NULL\n"+
            "AND\n"+
            "\tW1.F_WORK_STATUS = 81\n"+
            "AND\n"+
            "\tWL.F_PRODUCT_WORK_LINK_TYPE = 42\t-- includes\n";

    public static String GET_EPH_STG_PRODUCT_RELATIONSHIPS_COUNT = "select count(*) as count from ephsit.ephsit_talend_owner.stg_pmx_product_pack_rel\n";

    public static String GET_EPH_SA_PRODUCT_RELATIONSHIPS_COUNT = "select count(*) as count from semarchy_eph_mdm.sa_product_rel_package";

    public static String GET_EPH_GD_PRODUCT_RELATIONSHIPS_COUNT = "select count(*) as count from semarchy_eph_mdm.gd_product_rel_package";

    public static String GET_PMX_PRODUCT_RELATIONSHIPS_DATA = "SELECT \n" +
            "\t WL.PRODUCT_WORK_LINK_ID AS RELATIONSHIP_PMX_SOURCEREF \n" +
            "\t,W1.PRODUCT_WORK_ID || '-PKG' AS OWNER_PMX_SOURCE\n" +
            "\t,M2.PRODUCT_MANIFESTATION_ID || '-SUB' AS COMPONENT_PMX_SOURCE\n" +
            "\t,'CON' AS F_RELATIONSHIP_TYPE\n" +
            "\t,CASE WHEN W1.F_WORK_STATUS = 81 AND EFFFROM_DATE IS NULL THEN TO_DATE('2019-01-01', 'YYYY-MM-DD')\n" +
            "\t      WHEN W1.F_WORK_STATUS = 81 AND EFFFROM_DATE IS NOT NULL THEN EFFFROM_DATE\n" +
            "\t      ELSE NULL END AS EFFECTIVE_START_DATE  -- available work (81)\n" +
            "\t,NVL(W1.EFFECTIVE_TO_DATE, NVL(M2.EFFECTIVE_TO_DATE, WL.EFFTO_DATE)) AS ENDON\n" +
            "FROM \n" +
            "\tGD_PRODUCT_WORK_LINK WL,\n" +
            "\tGD_PRODUCT_WORK W1,\n" +
            "\tGD_PRODUCT_WORK W2,\n" +
            "\tGD_PRODUCT_MANIFESTATION M2\n" +
            "WHERE\n" +
            "\tWL.F_PRODUCT_WORK = W1.PRODUCT_WORK_ID\n" +
            "AND\t\n" +
            "\tWL.F_RELATED_PRODUCT_WORK = W2.PRODUCT_WORK_ID\n" +
            "AND\n" +
            "\tW2.PRODUCT_WORK_ID = M2.F_PRODUCT_WORK\n" +
            "AND\n" +
            "\tW1.F_PRODUCT_TYPE IN (21,143)\t\t-- full sets & subject collections\n" +
            "AND\t\n" +
            "\tW2.F_PRODUCT_TYPE = 4\t\t\t-- journals\n" +
            "AND\n" +
            "\tM2.F_PRODUCT_MANIFESTATION_TYP = 2  \t-- electronic \n" +
            "AND\n" +
            "\tNVL(W1.EFFECTIVE_TO_DATE, NVL(M2.EFFECTIVE_TO_DATE, WL.EFFTO_DATE)) IS NULL\n" +
            "AND\n" +
            "\tW1.F_WORK_STATUS = 81\n" +
            "AND\n" +
            "\tWL.F_PRODUCT_WORK_LINK_TYPE = 42\t-- includes\n" +
            "\t AND PRODUCT_WORK_LINK_ID in ('%s')";

    public static String GET_EPH_STG_PRODUCT_RELATIONSHIPS_DATA = "select\n" +
            "   \"RELATIONSHIP_PMX_SOURCEREF\" as RELATIONSHIP_PMX_SOURCEREF,\n" +
            "   \"OWNER_PMX_SOURCE\" as OWNER_PMX_SOURCE,\n" +
            "   \"COMPONENT_PMX_SOURCE\" as COMPONENT_PMX_SOURCE,\n" +
            "   \"F_RELATIONSHIP_TYPE\" as F_RELATIONSHIP_TYPE,\n" +
            "   \"EFFECTIVE_START_DATE\" as EFFECTIVE_START_DATE\n" +
            "   from ephsit.ephsit_talend_owner.stg_pmx_product_pack_rel\n" +
            "   where \"RELATIONSHIP_PMX_SOURCEREF\"  in ('%s')";

    public static String SELECT_RANDOM_RELATIONSHIP_PMX_SOURCEREF = "select \"RELATIONSHIP_PMX_SOURCEREF\" as RELATIONSHIP_PMX_SOURCEREF from ephsit_talend_owner.stg_pmx_product_pack_rel order by random() limit '%s'";

    public static String GET_PRODUCT_REL_PACK_ID_FROM_LOOKUP_TABLE = "select numeric_id as PRODUCT_REL_PACK_ID\n" +
            "FROM ephsit_talend_owner.map_sourceref_2_numericid where source_ref in ('%s')";

    public static String GET_RELATIONSHIP_PMX_SOURCEREF_FROM_LOOKUP_TABLE = "select numeric_id as PRODUCT_REL_PACK_ID\n" +
            "FROM ephsit_talend_owner.map_sourceref_2_numericid where source_ref in ('%s')";

    public static String GET_ID_FROM_SOURCEREF_LOOKUP_TABLE = "SELECT eph_id as eph_id\n" +
            "FROM ephsit_talend_owner.map_sourceref_2_ephid where source_ref in ('%s')";

    public static String GET_EPH_SA_PRODUCT_RELATIONSHIPS_DATA = "select \n" +
            "b_loadid as B_LOADID,\n" +
            "f_event as F_EVENT,\n" +
            "b_classname as B_CLASSNAME,\n" +
            "product_rel_pack_id as PRODUCT_REL_PACK_ID,\n" +
            "f_package_owner as F_PACKAGE_OWNER,\n" +
            "f_component as F_COMPONENT,\n" +
            "f_relationship_type as F_RELATIONSHIP_TYPE,\n" +
            "effective_start_date as EFFECTIVE_START_DATE,\n" +
            "effective_end_date as EFFECTIVE_END_DATE\n" +
            "from semarchy_eph_mdm.sa_product_rel_package\n" +
            "where \"product_rel_pack_id\"  in ('%s')";


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
            "where \"product_rel_pack_id\"  in ('%s')";


}
