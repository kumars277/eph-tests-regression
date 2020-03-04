package com.eph.automation.testing.services.db.DataLakeSql;

public  class WorksGDGHTablesDataChecksSQL {

    public static String GET_RANDOM_WORK_ID_GD = "select work_id as WORK_ID from semarchy_eph_mdm.gd_wwork order by random() limit '%s'";

    public static String GET_RANDOM_WORK_ID_GH = "select work_id as WORK_ID from semarchy_eph_mdm.gh_wwork order by random() limit '%s'";

    public static String GET_RANDOM_WORK_FIN_ATTR_ID_GD = "select work_fin_attribs_id as WORK_FIN_ATTRIBS_ID from semarchy_eph_mdm.gd_work_financial_attribs order by random() limit '%s'";

    public static String GET_RANDOM_WORK_IDENTFIER_ID_GD = "select work_identifier_id as WORK_IDENTIFIER_ID from semarchy_eph_mdm.gd_work_identifier order by random() limit '%s'";


    public  String gdWorkIdentifierBuildSql(String db) {
        String GET_DATA_WORK_IDENT_GD = "select \n" +
                "WORK_IDENTIFIER_ID as WORK_IDENTIFIER_ID\n" +
                ",B_CLASSNAME as B_CLASSNAME\n" +
                ",B_BATCHID as B_BATCHID\n" +
                ",B_CREDATE as B_CREDATE\n" +
                ",B_UPDDATE as B_UPDDATE\n" +
                ",B_CREATOR as B_CREATOR\n" +
                ",B_UPDATOR as B_UPDATOR\n" +
                ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
                ",IDENTIFIER as IDENTIFIER\n" +
                ",S_IDENTIFIER as S_IDENTIFIER\n" +
                ",EFFECTIVE_START_DATE as EFFECTIVE_START_DATE\n" +
                ",EFFECTIVE_END_DATE as EFFECTIVE_END_DATE\n" +
                ",F_TYPE as F_TYPE\n" +
                ",F_WWORK as F_WWORK\n" +
                ",F_EVENT as F_EVENT\n" +
                "from "+db+".gd_work_identifier\n" +
                "where WORK_IDENTIFIER_ID in ('%s')";

                return GET_DATA_WORK_IDENT_GD;

    }


    public  String gdWorkFinAttrBuildSql(String db) {

        String GET_DATA_FIN_ATTR_GD = "select \n" +
                "WORK_FIN_ATTRIBS_ID as WORK_FIN_ATTRIBS_ID\n" +
                ",B_CLASSNAME as B_CLASSNAME\n" +
                ",B_BATCHID as B_BATCHID\n" +
                ",B_CREDATE as B_CREDATE\n" +
                ",B_UPDDATE as B_UPDDATE\n" +
                ",B_CREATOR as B_CREATOR\n" +
                ",B_UPDATOR as B_UPDATOR\n" +
                ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
                ",EFFECTIVE_START_DATE as EFFECTIVE_START_DATE\n" +
                ",EFFECTIVE_END_DATE as EFFECTIVE_END_DATE\n" +
                ",F_GL_COMPANY as F_GL_COMPANY\n" +
                ",F_GL_COST_RESP_CENTRE as F_GL_COST_RESP_CENTRE\n" +
                ",F_GL_REVENUE_RESP_CENTRE as F_GL_REVENUE_RESP_CENTRE\n" +
                ",F_WWORK as F_WWORK\n" +
                ",F_EVENT as F_EVENT\n" +
                "from "+db+".gd_work_financial_attribs\n" +
                "where WORK_FIN_ATTRIBS_ID in ('%s')";

                return GET_DATA_FIN_ATTR_GD;

    }


    public  String gdWorkDataBuildSql(String db) {

        String GET_DATA_WORKS= "select \n" +
                "WORK_ID as WORK_ID\n" +
                ",B_CLASSNAME as B_CLASSNAME\n" +
                ",B_BATCHID as B_BATCHID\n" +
                ",B_CREDATE as B_CREDATE\n" +
                ",B_UPDDATE as B_UPDDATE\n" +
                ",B_CREATOR as B_CREATOR\n" +
                ",B_UPDATOR as B_UPDATOR\n" +
                ",S_WORK_ID as S_WORK_ID\n" +
                ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
                ",WORK_TITLE as WORK_TITLE\n" +
                ",S_WORK_TITLE as S_WORK_TITLE\n" +
                ",WORK_SUB_TITLE as WORK_SUB_TITLE\n" +
                ",S_WORK_SUB_TITLE as S_WORK_SUB_TITLE\n" +
                ",WORK_SHORT_TITLE as WORK_SHORT_TITLE\n" +
                ",S_WORK_SHORT_TITLE as S_WORK_SHORT_TITLE\n" +
                ",ELECTRO_RIGHTS_INDICATOR as ELECTRO_RIGHTS_INDICATOR\n" +
                ",VOLUME as VOLUME\n" +
                ",COPYRIGHT_YEAR as COPYRIGHT_YEAR\n" +
                ",EDITION_NUMBER as EDITION_NUMBER\n" +
                ",T_SUMMARY_CHANGED as T_SUMMARY_CHANGED\n" +
                ",T_EVENT_DESCRIPTION as T_EVENT_DESCRIPTION\n" +
                ",F_TYPE as F_TYPE\n" +
                ",F_STATUS as F_STATUS\n" +
                ",f_accountable_product as f_accountable_product\n" +
                ",F_PMC as F_PMC\n" +
                ",F_OA_TYPE as F_OA_TYPE\n" +
                ",F_FAMILY as F_FAMILY\n" +
                ",F_IMPRINT as F_IMPRINT\n" +
                ",F_SOCIETY_OWNERSHIP as F_SOCIETY_OWNERSHIP\n" +
                ",F_SUBSCRIPTION_TYPE as F_SUBSCRIPTION_TYPE\n" +
                ",F_LLANGUAGE as F_LLANGUAGE\n" +
                ",F_T_EVENT_TYPE as F_T_EVENT_TYPE\n" +
                ",F_EVENT as F_EVENT\n" +
                ",F_SELF_ONE as F_SELF_ONE\n" +
                ",F_SELF_TWO as F_SELF_TWO\n" +
                ",F_SELF_THREE as F_SELF_THREE\n" +
                ",F_SELF_FOUR as F_SELF_FOUR\n" +
                ",F_SELF_FIVE as F_SELF_FIVE\n" +
                ",F_SELF_SIX as F_SELF_SIX\n" +
                ",F_SELF_SEVEN as F_SELF_SEVEN\n" +
                ",F_SELF_EIGHT as F_SELF_EIGHT\n" +
                "from "+db+".gd_wwork\n" +
                "where WORK_ID in ('%s')";

        return GET_DATA_WORKS;
    }

    public  String ghWorkDataBuildSql(String db) {
        String GET_DATA_GH_WORKS = "select \n" +
                "WORK_ID as WORK_ID\n" +
                ",B_FROMBATCHID as B_FROMBATCHID\n" +
                ",B_TOBATCHID as B_TOBATCHID\n" +
                ",B_CLASSNAME as B_CLASSNAME\n" +
                ",B_CREDATE as B_CREDATE\n" +
                ",B_UPDDATE as B_UPDDATE\n" +
                ",B_CREATOR as B_CREATOR\n" +
                ",B_UPDATOR as B_UPDATOR\n" +
                ",S_WORK_ID as S_WORK_ID\n" +
                ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
                ",WORK_TITLE as WORK_TITLE\n" +
                ",S_WORK_TITLE as S_WORK_TITLE\n" +
                ",WORK_SUB_TITLE as WORK_SUB_TITLE\n" +
                ",S_WORK_SUB_TITLE as S_WORK_SUB_TITLE\n" +
                ",WORK_SHORT_TITLE as WORK_SHORT_TITLE\n" +
                ",S_WORK_SHORT_TITLE as S_WORK_SHORT_TITLE\n" +
                ",ELECTRO_RIGHTS_INDICATOR as ELECTRO_RIGHTS_INDICATOR\n" +
                ",VOLUME as VOLUME\n" +
                ",COPYRIGHT_YEAR as COPYRIGHT_YEAR\n" +
                ",EDITION_NUMBER as EDITION_NUMBER\n" +
                ",T_SUMMARY_CHANGED as T_SUMMARY_CHANGED\n" +
                ",T_EVENT_DESCRIPTION as T_EVENT_DESCRIPTION\n" +
                ",F_TYPE as F_TYPE\n" +
                ",F_STATUS as F_STATUS\n" +
                ",f_accountable_product as f_accountable_product\n" +
                ",F_PMC as F_PMC\n" +
                ",F_OA_TYPE as F_OA_TYPE\n" +
                ",F_FAMILY as F_FAMILY\n" +
                ",F_IMPRINT as F_IMPRINT\n" +
                ",F_SOCIETY_OWNERSHIP as F_SOCIETY_OWNERSHIP\n" +
                ",F_SUBSCRIPTION_TYPE as F_SUBSCRIPTION_TYPE\n" +
                ",F_LLANGUAGE as F_LLANGUAGE\n" +
                ",F_T_EVENT_TYPE as F_T_EVENT_TYPE\n" +
                ",F_EVENT as F_EVENT\n" +
                ",F_SELF_ONE as F_SELF_ONE\n" +
                ",F_SELF_TWO as F_SELF_TWO\n" +
                ",F_SELF_THREE as F_SELF_THREE\n" +
                ",F_SELF_FOUR as F_SELF_FOUR\n" +
                ",F_SELF_FIVE as F_SELF_FIVE\n" +
                ",F_SELF_SIX as F_SELF_SIX\n" +
                ",F_SELF_SEVEN as F_SELF_SEVEN\n" +
                ",F_SELF_EIGHT as F_SELF_EIGHT\n" +
                "from "+db+".gh_wwork \n" +
                "where WORK_ID in ('%s')";

        return GET_DATA_GH_WORKS;



    }




}
