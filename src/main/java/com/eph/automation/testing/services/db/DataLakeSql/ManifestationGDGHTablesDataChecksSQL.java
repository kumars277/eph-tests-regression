package com.eph.automation.testing.services.db.DataLakeSql;

public  class ManifestationGDGHTablesDataChecksSQL {

    public static String GET_RANDOM_MANIFESTATION_ID = "select manifestation_id as MANIFESTATION_ID from semarchy_eph_mdm.gd_manifestation order by random() limit '%s'";

    public static String GET_RANDOM_MANIFESTATION_ID_GH = "select manifestation_id as MANIFESTATION_ID from semarchy_eph_mdm.gh_manifestation order by random() limit '%s'";

    public static String  GET_RANDOM_MANI_IDENTIFIER_ID_GH = "select manif_identifier_id as MANIF_IDENTIFIER_ID from semarchy_eph_mdm.gh_manifestation_identifier order by random() limit '%s'";

    public static String  GET_RANDOM_MANI_IDENTIFIER_ID_GD = "select manif_identifier_id as MANIF_IDENTIFIER_ID from semarchy_eph_mdm.gd_manifestation_identifier order by random() limit '%s'";

    public String gdManifestationBuildSql(String db){
        String GET_DATA_MANIFESTATION = "select \n" +
                "MANIFESTATION_ID as MANIFESTATION_ID\n" +
                ",B_CLASSNAME as B_CLASSNAME\n" +
                ",B_BATCHID as B_BATCHID\n" +
                ",B_CREDATE as B_CREDATE\n" +
                ",B_UPDDATE as B_UPDDATE\n" +
                ",B_CREATOR as B_CREATOR\n" +
                ",B_UPDATOR as B_UPDATOR\n" +
                ",S_MANIFESTATION_ID as S_MANIFESTATION_ID\n" +
                ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
                ",MANIFESTATION_KEY_TITLE as MANIFESTATION_KEY_TITLE\n" +
                ",S_MANIFESTATION_KEY_TITLE as S_MANIFESTATION_KEY_TITLE\n" +
                ",INTER_EDITION_FLAG as INTER_EDITION_FLAG\n" +
                ",FIRST_PUB_DATE as FIRST_PUB_DATE\n" +
                ",LAST_PUB_DATE as LAST_PUB_DATE\n" +
                ",T_EVENT_DESCRIPTION as T_EVENT_DESCRIPTION\n" +
                ",F_TYPE as F_TYPE\n" +
                ",F_STATUS as F_STATUS\n" +
                ",F_FORMAT_TYPE as F_FORMAT_TYPE\n" +
                ",F_WWORK as F_WWORK\n" +
                ",F_T_EVENT_TYPE as F_T_EVENT_TYPE\n" +
                ",F_EVENT as F_EVENT\n" +
                ",F_SELF_ONE as F_SELF_ONE\n" +
                "from "+db+".gd_manifestation \n" +
                "where MANIFESTATION_ID in ('%s')";

        return GET_DATA_MANIFESTATION;
    }

    public String ghManifestationBuildSql(String db){
        String GET_DATA_GH_MANIFESTATION = "select \n" +
                "MANIFESTATION_ID as MANIFESTATION_ID\n" +
                ",B_CLASSNAME as B_CLASSNAME\n" +
                ",B_FROMBATCHID as B_FROMBATCHID\n" +
                ",B_TOBATCHID as B_TOBATCHID\n" +
                ",B_CREDATE as B_CREDATE\n" +
                ",B_UPDDATE as B_UPDDATE\n" +
                ",B_CREATOR as B_CREATOR\n" +
                ",B_UPDATOR as B_UPDATOR\n" +
                ",S_MANIFESTATION_ID as S_MANIFESTATION_ID\n" +
                ",EXTERNAL_REFERENCE as EXTERNAL_REFERENCE\n" +
                ",MANIFESTATION_KEY_TITLE as MANIFESTATION_KEY_TITLE\n" +
                ",S_MANIFESTATION_KEY_TITLE as S_MANIFESTATION_KEY_TITLE\n" +
                ",INTER_EDITION_FLAG as INTER_EDITION_FLAG\n" +
                ",FIRST_PUB_DATE as FIRST_PUB_DATE\n" +
                ",LAST_PUB_DATE as LAST_PUB_DATE\n" +
                ",T_EVENT_DESCRIPTION as T_EVENT_DESCRIPTION\n" +
                ",F_TYPE as F_TYPE\n" +
                ",F_STATUS as F_STATUS\n" +
                ",F_FORMAT_TYPE as F_FORMAT_TYPE\n" +
                ",F_WWORK as F_WWORK\n" +
                ",F_T_EVENT_TYPE as F_T_EVENT_TYPE\n" +
                ",F_EVENT as F_EVENT\n" +
                ",F_SELF_ONE as F_SELF_ONE\n" +
                "from "+db+".gh_manifestation \n" +
                "where MANIFESTATION_ID in ('%s')";
        return GET_DATA_GH_MANIFESTATION;

    }


    public String ghManifIdentifierBuildSql(String db){
        String GET_DATA_GH_MANI_IDENTIFIER = "select \n" +
                "MANIF_IDENTIFIER_ID as MANIF_IDENTIFIER_ID\n" +
                ",B_FROMBATCHID as B_FROMBATCHID\n" +
                ",B_TOBATCHID as B_TOBATCHID\n" +
                ",B_CLASSNAME as B_CLASSNAME\n" +
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
                ",F_MANIFESTATION as F_MANIFESTATION\n" +
                ",F_EVENT as F_EVENT\n" +
                "from "+db+".gh_manifestation_identifier \n" +
                "where MANIF_IDENTIFIER_ID in ('%s')";

        return GET_DATA_GH_MANI_IDENTIFIER;
    }

    public String gdManifIdentifierBuildSql(String db){
        String GET_DATA_GD_MANI_IDENTIFIER=  "select \n" +
                "MANIF_IDENTIFIER_ID as MANIF_IDENTIFIER_ID\n" +
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
                ",F_MANIFESTATION as F_MANIFESTATION\n" +
                ",F_EVENT as F_EVENT\n" +
                "from "+db+".gd_manifestation_identifier \n" +
                "where MANIF_IDENTIFIER_ID in ('%s')";

        return GET_DATA_GD_MANI_IDENTIFIER;

    }


}
