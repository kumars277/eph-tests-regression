package com.eph.automation.testing.services.db.EPHDataLakeSql;

public class GetDLDBUser {

    public static String getDataBase(){
        String dbDL  = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbDL = "product_database_sit";
            }
            else{
                dbDL = "product_database_uat";
            }

        }else{
            dbDL = "product_database_sit";
        }
        return dbDL;
    }
}
