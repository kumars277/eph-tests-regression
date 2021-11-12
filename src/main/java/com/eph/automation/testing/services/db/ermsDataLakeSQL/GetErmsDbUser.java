package com.eph.automation.testing.services.db.ermsDataLakeSQL;
import com.eph.automation.testing.helper.Log;

public class GetErmsDbUser {

    private GetErmsDbUser(){
        Log.info("erms User");}

    public static String getERMSDataBase(){
        String dbERMS = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbERMS = "erms_staging_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbERMS = "erms_staging_uat";
            }
        }else{
            dbERMS = "erms_staging_uat";
        }
        return dbERMS;
    }

    public static String getProdStagingDataBase(){

        String dbProdstgDb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdstgDb = "product_staging_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdstgDb = "product_staging_database_uat";
            }
        }else{
            dbProdstgDb = "product_staging_database_uat";
        }
        return dbProdstgDb;
    }

    public static String getProdDataBase(){

        String dbProdDb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdDb = "product_database_sit";
            }
            else{
                dbProdDb = "product_database_uat";
            }
        }else{
             dbProdDb = "product_database_uat";
        }
        return dbProdDb;
    }


}


