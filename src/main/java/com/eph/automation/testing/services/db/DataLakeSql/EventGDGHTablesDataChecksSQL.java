package com.eph.automation.testing.services.db.DataLakeSql;

public  class EventGDGHTablesDataChecksSQL {

    public static String GET_RANDOM_EVENT_ID_GD = "select event_id as EVENT_ID from semarchy_eph_mdm.gd_event order by random() limit '%s'";

    public String eventBuildSQLQuery(String db){

        String GET_DATA_EVENT = "select \n" +
                "EVENT_ID as EVENT_ID\n" +
                ",B_CLASSNAME as B_CLASSNAME\n" +
                ",B_BATCHID as B_BATCHID\n" +
                ",B_CREDATE as B_CREDATE\n" +
                ",B_UPDDATE as B_UPDDATE\n" +
                ",B_CREATOR as B_CREATOR\n" +
                ",B_UPDATOR as B_UPDATOR\n" +
                ",DDATE as DDATE\n" +
                ",TTIMESTAMP as TTIMESTAMP\n" +
                ",DESCRIPTION as DESCRIPTION\n" +
                ",THRID_PARTY as THIRD_PARTY\n" +
                ",WORKFLOW_ID as WORKFLOW_ID\n" +
                ",F_EVENT_TYPE as F_EVENT_TYPE\n" +
                ",F_WORKFLOW_SOURCE as F_WORKFLOW_SOURCE\n" +
                ",F_SELF_ONE as F_SELF_ONE\n" +
                ",F_SELF_TWO as F_SELF_TWO\n" +
                ",F_SELF_THREE as F_SELF_THREE\n" +
                ",F_SELF_FOUR as F_SELF_FOUR\n" +
                "from "+db+".gd_event\n" +
                " where EVENT_ID in ('%s')";

        return GET_DATA_EVENT;
    }

}
