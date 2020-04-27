package com.eph.automation.testing.services.db.PROMISDataLakeSQL;

public class GetPRMDLDBUser {

    public static String getPRMDataBase(){
        String dbPRMDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbPRMDL = "promis_staging_sit";
            }
            else{
                dbPRMDL = "promis_staging_uat";
            }

        }else{
            dbPRMDL = "promis_staging_sit";
        }
        return dbPRMDL;
    }
}
