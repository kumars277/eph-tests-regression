package com.eph.automation.testing.services.db.PROMISDataLakeSQL;

public class GetPRMDLDBUser {

    public static String getPRMDataBase(){
        String dbPRMDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbPRMDL = "promis_staging_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
               dbPRMDL = "promis_staging_uat";
          }
        }else{
           dbPRMDL = "promis_staging_sit";
        }
        return dbPRMDL;
    }
    public static String getEphCrossRefDataBase(){
        String dbEphCrossRef = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbEphCrossRef = "eph_sit_crossreference_database";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbEphCrossRef = "eph_uat_crossreference_database";
            }
        }else{
            dbEphCrossRef = "eph_sit_crossreference_database";
            // dbEphCrossRef = "eph_uat_crossreference_database";
        }
        return dbEphCrossRef;
    }
    public static String getProdDataBase(){
        String dbProdDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdDL = "product_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdDL = "product_database_uat";
            }
        }else{
            dbProdDL = "product_database_sit";
           // dbProdDL = "product_database_uat";
        }
        return dbProdDL;
    }

    public static String getProdStagingDataBase(){
        String dbStageProdDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbStageProdDL = "product_staging_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbStageProdDL = "product_staging_database_uat";
            }
        }else{
            dbStageProdDL = "product_staging_database_sit";
         //   dbStageProdDL = "product_staging_database_uat";
        }
        return dbStageProdDL;
    }


}
