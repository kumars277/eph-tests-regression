package com.eph.automation.testing.services.db.BCS_ETLCoreSQL;

public class GetBCS_ETLCoreDLDBUser {

    public static String getBCS_ETLCoreDataBase(){
        String dbBCS = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbBCS = "bcs_ingestion_database_sit";
            }
            else{
                dbBCS = "bcs_ingestion_database_uat";
            }

        }else{
            dbBCS = "bcs_ingestion_database_sit";
          //  dbSDDL = "bcs_ingestion_database_uat";
        }
        return dbBCS;
    }


}
