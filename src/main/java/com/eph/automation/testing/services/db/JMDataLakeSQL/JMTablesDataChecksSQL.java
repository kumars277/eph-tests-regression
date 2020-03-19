package com.eph.automation.testing.services.db.JMDataLakeSQL;

import com.eph.automation.testing.configuration.Constants;

public class JMTablesDataChecksSQL {

    public static String GET_ALLOCATION_IDS = "select ALLOCATION_CHANGE_ID from jmf_sit_application.JMF_ALLOCATION_CHANGE order by rand() limit %s;";


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



}
