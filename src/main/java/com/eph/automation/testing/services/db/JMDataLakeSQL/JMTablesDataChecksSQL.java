package com.eph.automation.testing.services.db.JMDataLakeSQL;

import com.eph.automation.testing.configuration.Constants;

public class JMTablesDataChecksSQL {

    public static String GET_ALLOCATION_IDS = "select ALLOCATION_CHANGE_ID from "+Constants.JMF_SQL_SCHEMA+".JMF_ALLOCATION_CHANGE order by rand() limit %s";

    public static String GET_APPLICATION_KEY = "select APPLICATION_PROPERTY_KEY from "+Constants.JMF_SQL_SCHEMA+".JMF_APPLICATION_PROPERTIES order by rand() limit %s";

    public static String GET_APPROVAL_REQ_ID = "select APPROVAL_ID from "+Constants.JMF_SQL_SCHEMA+".JMF_APPROVAL_REQUEST order by rand() limit %s";

    public static String GET_CHRONICLE_SCE_ID = "select CHRONICLE_SCENARIO_CODE from "+Constants.JMF_SQL_SCHEMA+".JMF_CHRONICLE_SCENARIO order by rand() limit %s";

    public static String GET_CHRONICLE_STATUS_ID = "select CHRONICLE_STATUS_CODE from "+Constants.JMF_SQL_SCHEMA+".JMF_CHRONICLE_STATUS order by rand() limit %s";

    public static String GET_FAMILY_RESOURCE_ID = "select FAMILY_RESOURCE_DETAILS_ID from "+Constants.JMF_SQL_SCHEMA+".JMF_FAMILY_RESOURCE_DETAILS order by rand() limit %s";


    public static String getAllocationChangesSql(String serverEnv, String table){
        String GET_DATA_ALLOCATION_CHANGE_JM = null;
        switch (serverEnv){
            case ("mysql"):
            GET_DATA_ALLOCATION_CHANGE_JM = "select \n" +
                    "ALLOCATION_CHANGE_ID as ALLOCATION_CHANGE_ID\n" +
                    ",ALLOCATION_TYPE as ALLOCATION_TYPE\n" +
                    ",PMG_FILTER as PMG_FILTER\n" +
                    ",PMC_FILTER as PMC_FILTER\n" +
                    ",PPC_FILTER_EMAIL as PPC_FILTER_EMAIL\n" +
                    ",PPC_FILTER_NAME as PPC_FILTER_NAME\n" +
                    ",PMC_CHANGE_IND as PMC_CHANGE_IND\n" +
                    ",PPC_CHANGE_IND as PPC_CHANGE_IND\n" +
                    ",PMX_PMGCODE as PMX_PMGCODE\n" +
                    ",PMX_PMG_NAME as PMX_PMG_NAME\n" +
                    ",PMX_PMG_F_BUSINESS_UNIT as PMX_PMG_F_BUSINESS_UNIT\n" +
                    ",PMG_BUS_CTRL_NAME as PMG_BUS_CTRL_NAME\n" +
                    ",PMG_BUS_CTRL_EMAIL as PMG_BUS_CTRL_EMAIL\n" +
                    ",PMG_PUBDIR_NAME_OLD as PMG_PUBDIR_NAME_OLD\n" +
                    ",PMG_PUBDIR_EMAIL_OLD as PMG_PUBDIR_EMAIL_OLD\n" +
                    ",PMG_PUBDIR_PEOPLE_HUB_ID_OLD as PMG_PUBDIR_PEOPLE_HUB_ID_OLD\n" +
                    ",PMG_PUBDIR_NAME_NEW as PMG_PUBDIR_NAME_NEW\n" +
                    ",PMG_PUBDIR_EMAIL_NEW as PMG_PUBDIR_EMAIL_NEW\n"+
                    ",PMG_PUBDIR_PEOPLE_HUB_ID_NEW as PMG_PUBDIR_PEOPLE_HUB_ID_NEW\n" +
                    ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                    ",EPH_PMG_CODE asEPH_PMG_CODE\n" +
                    "from "+Constants.JMF_SQL_SCHEMA+"."+table+"\n" +
                    " where ALLOCATION_CHANGE_ID in ('%s')";
                    break;
            case ("aws"):
                GET_DATA_ALLOCATION_CHANGE_JM = "select \n" +
                        "ALLOCATION_CHANGE_ID as ALLOCATION_CHANGE_ID\n" +
                        ",ALLOCATION_TYPE as ALLOCATION_TYPE\n" +
                        ",PMG_FILTER as PMG_FILTER\n" +
                        ",PMC_FILTER as PMC_FILTER\n" +
                        ",PPC_FILTER_EMAIL as PPC_FILTER_EMAIL\n" +
                        ",PPC_FILTER_NAME as PPC_FILTER_NAME\n" +
                        ",PMC_CHANGE_IND as PMC_CHANGE_IND\n" +
                        ",PPC_CHANGE_IND as PPC_CHANGE_IND\n" +
                        ",PMX_PMGCODE as PMX_PMGCODE\n" +
                        ",PMX_PMG_NAME as PMX_PMG_NAME\n" +
                        ",PMX_PMG_F_BUSINESS_UNIT as PMX_PMG_F_BUSINESS_UNIT\n" +
                        ",PMG_BUS_CTRL_NAME as PMG_BUS_CTRL_NAME\n" +
                        ",PMG_BUS_CTRL_EMAIL as PMG_BUS_CTRL_EMAIL\n" +
                        ",PMG_PUBDIR_NAME_OLD as PMG_PUBDIR_NAME_OLD\n" +
                        ",PMG_PUBDIR_EMAIL_OLD as PMG_PUBDIR_EMAIL_OLD\n" +
                        ",PMG_PUBDIR_PEOPLE_HUB_ID_OLD as PMG_PUBDIR_PEOPLE_HUB_ID_OLD\n" +
                        ",PMG_PUBDIR_NAME_NEW as PMG_PUBDIR_NAME_NEW\n" +
                        ",PMG_PUBDIR_EMAIL_NEW as PMG_PUBDIR_EMAIL_NEW\n"+
                        ",PMG_PUBDIR_PEOPLE_HUB_ID_NEW as PMG_PUBDIR_PEOPLE_HUB_ID_NEW\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        ",EPH_PMG_CODE asEPH_PMG_CODE\n" +
                        "from "+Constants.JMF_AWS_SCHEMA+"."+table+"\n" +
                        " where inbound_ts = (select inbound_ts from "+Constants.JMF_AWS_SCHEMA+"."+table+" order by inbound_ts desc limit 1)\n" +
                        " AND ALLOCATION_CHANGE_ID in ('%s')";
                break;
        }
        return GET_DATA_ALLOCATION_CHANGE_JM;

    }


    public static String getAppPropertySql(String serverEnv, String table){
        String GET_DATA_APP_PROP_JM = null;
        switch (serverEnv){
            case ("mysql"):
                GET_DATA_APP_PROP_JM = "select \n" +
                        "APPLICATION_PROPERTY_KEY as APPLICATION_PROPERTY_KEY\n" +
                        ",APPLICATION_PROPERTY_VALUE as APPLICATION_PROPERTY_VALUE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from "+Constants.JMF_SQL_SCHEMA+"."+table+"\n" +
                        " where APPLICATION_PROPERTY_KEY in ('%s')";
                break;
            case ("aws"):
                GET_DATA_APP_PROP_JM = "select \n" +
                        "APPLICATION_PROPERTY_KEY as APPLICATION_PROPERTY_KEY\n" +
                        ",APPLICATION_PROPERTY_VALUE as APPLICATION_PROPERTY_VALUE\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from "+Constants.JMF_AWS_SCHEMA+"."+table+"\n" +
                        " where inbound_ts = (select inbound_ts from "+Constants.JMF_AWS_SCHEMA+"."+table+" order by inbound_ts desc limit 1)\n" +
                        " AND APPLICATION_PROPERTY_KEY in ('%s')";
                break;

        }
        return GET_DATA_APP_PROP_JM;

    }

    public static String getAppRequestSql(String serverEnv, String table){
        String GET_DATA_APP_REQ_JM = null;
        switch (serverEnv){
            case ("mysql"):
                GET_DATA_APP_REQ_JM = "select \n" +
                        "APPROVAL_ID as APPROVAL_ID\n" +
                        ",F_PRODUCT_CHRONICLE as F_PRODUCT_CHRONICLE\n" +
                        ",APPROVAL_TYPE as APPROVAL_TYPE\n" +
                        ",APPROVER_NAME as APPROVER_NAME\n" +
                        ",APPROVAL_GIVEN_INDICATOR as APPROVAL_GIVEN_INDICATOR\n" +
                        ",APPROVAL_REQUEST_DATE as APPROVAL_REQUEST_DATE\n" +
                        ",APPROVAL_RESPONSE_DATE as APPROVAL_RESPONSE_DATE\n" +
                        "from "+Constants.JMF_SQL_SCHEMA+"."+table+"\n" +
                        " where APPROVAL_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_APP_REQ_JM = "select \n" +
                        "APPROVAL_ID as APPROVAL_ID\n" +
                        ",F_PRODUCT_CHRONICLE as F_PRODUCT_CHRONICLE\n" +
                        ",APPROVAL_TYPE as APPROVAL_TYPE\n" +
                        ",APPROVER_NAME as APPROVER_NAME\n" +
                        ",APPROVAL_GIVEN_INDICATOR as APPROVAL_GIVEN_INDICATOR\n" +
                        ",APPROVAL_REQUEST_DATE as APPROVAL_REQUEST_DATE\n" +
                        ",APPROVAL_RESPONSE_DATE as APPROVAL_RESPONSE_DATE\n" +
                        "from "+Constants.JMF_AWS_SCHEMA+"."+table+"\n" +
                        " where inbound_ts = (select inbound_ts from "+Constants.JMF_AWS_SCHEMA+"."+table+" order by inbound_ts desc limit 1)\n" +
                        " AND APPROVAL_ID in ('%s')";
                break;

        }
        return GET_DATA_APP_REQ_JM;
    }

    public static String getChronicleSceSql(String serverEnv, String table){
        String GET_DATA_CHRO_SCE_JM = null;
        switch (serverEnv){
            case ("mysql"):
                GET_DATA_CHRO_SCE_JM = "select \n" +
                        "CHRONICLE_SCENARIO_CODE as CHRONICLE_SCENARIO_CODE\n" +
                        ",CHRONICLE_SCENARIO_NAME as CHRONICLE_SCENARIO_NAME\n" +
                        ",CHRONICLE_SCENARIO_DESCRIPTION as CHRONICLE_SCENARIO_DESCRIPTION\n" +
                        ",CHRONICLE_SCENARIO_EVOLUTIONARY_IND as CHRONICLE_SCENARIO_EVOLUTIONARY_IND\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from "+Constants.JMF_SQL_SCHEMA+"."+table+"\n" +
                        " where CHRONICLE_SCENARIO_CODE in ('%s')";
                break;
            case ("aws"):
                GET_DATA_CHRO_SCE_JM = "select \n" +
                        "CHRONICLE_SCENARIO_CODE as CHRONICLE_SCENARIO_CODE\n" +
                        ",CHRONICLE_SCENARIO_NAME as CHRONICLE_SCENARIO_NAME\n" +
                        ",CHRONICLE_SCENARIO_DESCRIPTION as CHRONICLE_SCENARIO_DESCRIPTION\n" +
                        ",CHRONICLE_SCENARIO_EVOLUTIONARY_IND as CHRONICLE_SCENARIO_EVOLUTIONARY_IND\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from "+Constants.JMF_AWS_SCHEMA+"."+table+"\n" +
                        " where inbound_ts = (select inbound_ts from "+Constants.JMF_AWS_SCHEMA+"."+table+" order by inbound_ts desc limit 1)\n" +
                        " AND CHRONICLE_SCENARIO_CODE in ('%s')";
                break;

        }
        return GET_DATA_CHRO_SCE_JM;

    }

    public static String getChronicleStatusSql(String serverEnv, String table){
        String GET_DATA_CHRO_STATUS_JM = null;
        switch (serverEnv){
            case ("mysql"):
                GET_DATA_CHRO_STATUS_JM = "select \n" +
                        "CHRONICLE_STATUS_CODE as CHRONICLE_STATUS_CODE\n" +
                        ",CHRONICLE_STATUS_NAME as CHRONICLE_STATUS_NAME\n" +
                        ",CHRONICLE_STATUS_DESCRIPTION as CHRONICLE_STATUS_DESCRIPTION\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from "+Constants.JMF_SQL_SCHEMA+"."+table+"\n" +
                        " where CHRONICLE_STATUS_CODE in ('%s')";
                break;
            case ("aws"):
                GET_DATA_CHRO_STATUS_JM = "select \n" +
                        "CHRONICLE_STATUS_CODE as CHRONICLE_STATUS_CODE\n" +
                        ",CHRONICLE_STATUS_NAME as CHRONICLE_STATUS_NAME\n" +
                        ",CHRONICLE_STATUS_DESCRIPTION as CHRONICLE_STATUS_DESCRIPTION\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from "+Constants.JMF_AWS_SCHEMA+"."+table+"\n" +
                        " where inbound_ts = (select inbound_ts from "+Constants.JMF_AWS_SCHEMA+"."+table+" order by inbound_ts desc limit 1)\n" +
                        " AND CHRONICLE_STATUS_CODE in ('%s')";
                break;

        }
        return GET_DATA_CHRO_STATUS_JM;

    }

    public static String getFamilyResourceSql(String serverEnv, String table){
        String GET_DATA_FAMILY_RESOURCE_JM = null;
        switch (serverEnv){
            case ("mysql"):
                GET_DATA_FAMILY_RESOURCE_JM = "select \n" +
                        "FAMILY_RESOURCE_DETAILS_ID as FAMILY_RESOURCE_DETAILS_ID\n" +
                        ",RESOURCE_KEY as RESOURCE_KEY\n" +
                        ",INITIAL_VALUE as INITIAL_VALUE\n" +
                        ",RESOURCE_CHANGE_INDICATOR as RESOURCE_CHANGE_INDICATOR\n" +
                        ",NEW_VALUE as NEW_VALUE\n" +
                        ",F_PRODUCT_FAMILY as F_PRODUCT_FAMILY\n" +
                        ",PEOPLEHUB_ID_OLD as PEOPLEHUB_ID_OLD\n" +
                        ",PEOPLEHUB_ID_NEW as PEOPLEHUB_ID_NEW\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from "+Constants.JMF_SQL_SCHEMA+"."+table+"\n" +
                        " where FAMILY_RESOURCE_DETAILS_ID in ('%s')";
                break;
            case ("aws"):
                GET_DATA_FAMILY_RESOURCE_JM = "select \n" +
                        "FAMILY_RESOURCE_DETAILS_ID as FAMILY_RESOURCE_DETAILS_ID\n" +
                        ",RESOURCE_KEY as RESOURCE_KEY\n" +
                        ",INITIAL_VALUE as INITIAL_VALUE\n" +
                        ",RESOURCE_CHANGE_INDICATOR as RESOURCE_CHANGE_INDICATOR\n" +
                        ",NEW_VALUE as NEW_VALUE\n" +
                        ",F_PRODUCT_FAMILY as F_PRODUCT_FAMILY\n" +
                        ",PEOPLEHUB_ID_OLD as PEOPLEHUB_ID_OLD\n" +
                        ",PEOPLEHUB_ID_NEW as PEOPLEHUB_ID_NEW\n" +
                        ",NOTIFIED_DATE as NOTIFIED_DATE\n" +
                        "from "+Constants.JMF_AWS_SCHEMA+"."+table+"\n" +
                        " where inbound_ts = (select inbound_ts from "+Constants.JMF_AWS_SCHEMA+"."+table+" order by inbound_ts desc limit 1)\n" +
                        " AND FAMILY_RESOURCE_DETAILS_ID in ('%s')";
                break;

        }
        return GET_DATA_FAMILY_RESOURCE_JM;

    }



}
