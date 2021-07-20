package com.eph.automation.testing.services.db.sdrmsql;
import com.eph.automation.testing.helper.Log;

public class GetSdrmDbUser {

    private GetSdrmDbUser(){
        Log.info("sdrmdb User");}

    public static String getSDRMDataBase(){
        String dbSDRM = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbSDRM = "sdrm_staging_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbSDRM = "sdrm_staging_uat";
            }
        }else{
            dbSDRM = "sdrm_staging_uat";
        }
        return dbSDRM;
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


    public static String getProdExtDataBase(){

        String dbProdstgDb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdstgDb = "product_ext_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdstgDb = "product_ext_database_uat";
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


