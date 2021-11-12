package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class GetCKDLDB {
    public static String getCKDataBase(){
        String dbCKDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbCKDL = "dpp_staging_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbCKDL = "dpp_staging_uat";
            }
        }else{
            // dbCKDL = "dpp_staging_sit";
            dbCKDL = "dpp_staging_sit";
        }
        return dbCKDL;
    }
}
