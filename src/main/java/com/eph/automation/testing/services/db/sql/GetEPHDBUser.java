package com.eph.automation.testing.services.db.sql;

public class GetEPHDBUser {

    public static String getDBUser(){
        String dbUser = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbUser = "ephsit_talend_owner";
            }else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbUser = "ephuat_talend_owner";
            }

        }else {
            dbUser = "ephsit_talend_owner";
//            dbUser = "ephuat_talend_owner";

        }
        return dbUser;
    }
}
