package com.eph.automation.testing.services.db.bcsetlcoresql;

import com.eph.automation.testing.helper.Log;

public class GetBcsEtlCoreDLDBUser {

    private GetBcsEtlCoreDLDBUser(){
        Log.info("implicit public");
    }

    public static String getBcsETLCoreDataBase(){
        String dbBCS = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbBCS = "bcs_ingestion_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbBCS = "bcs_ingestion_database_uat";
            }

        }else{
            dbBCS = "bcs_ingestion_database_uat";
        }
        return dbBCS;
    }

    public static String getErmsETLCoreDataBase(){
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

    public static String getProdDataBase(){
        String dbProdDB = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdDB = "product_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdDB = "product_database_uat";
            }
        }else{
               dbProdDB = "product_database_uat";
        }
        return dbProdDB;
    }

    public static String getdppDataBase(){
        String dbDpp = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbDpp = "dpp_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbDpp = "dpp_database_uat";
            }
        }else{
             dbDpp = "dpp_database_uat";
        }
        return dbDpp;
    }
    public static String getJmCoreDataBase(){
        String dbJM = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJM = "journalmaestro_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbJM = "journalmaestro_uat";
            }
        }else{
              dbJM = "journalmaestro_uat";
        }
        return dbJM;
    }

    public static String getDlCoreViewDataBase(){
        String dbProd = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProd = "product_staging_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProd = "product_staging_database_uat";
            }

        }else{
            dbProd = "product_staging_database_uat";
        }
        return dbProd;
    }

    public static String getProdDataBaseGd(){
        String dbProdDb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdDb = "product_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdDb = "product_database_uat";
            }

        }else{
            dbProdDb = "product_database_uat";
        }
        return dbProdDb;
    }

}
