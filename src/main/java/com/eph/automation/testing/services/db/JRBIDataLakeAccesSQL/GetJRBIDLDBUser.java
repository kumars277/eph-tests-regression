package com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL;

public class GetJRBIDLDBUser {

    public static String getJRBIDataBase(){
        String dbJRBIDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJRBIDL = "jrbi_staging_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbJRBIDL = "jrbi_staging_uat";
            }
        }else{
           //dbJRBIDL = "jrbi_staging_sit";
            dbJRBIDL = "jrbi_staging_uat";
        }
        return dbJRBIDL;
    }

    public static String getProductDatabase(){
        String dbProdDb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdDb = "product_staging_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdDb = "product_staging_database_uat";
            }else{
                dbProdDb = "product_staging_database_uat2";
            }
        }else{
          //dbProdDb = "product_staging_database_sit";
          dbProdDb = "product_staging_database_uat";
        }
        return dbProdDb;
    }

    public static String getProductGDdb(){
        String dbProdGDdb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdGDdb = "product_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdGDdb = "product_database_uat";
            }
        }else{
           // dbProdGDdb = "product_database_sit";
            dbProdGDdb = "product_database_uat";
        }
        return dbProdGDdb;
    }

    public static String getProductExtdb(){
        String dbProdExtdb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdExtdb = "product_ext_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdExtdb = "product_ext_database_uat";
            }
         }else{
          //dbProdExtdb = "product_ext_database_sit";
           dbProdExtdb = "product_ext_database_uat";
        }
        return dbProdExtdb;
    }

    public static String getStitchingdb(){
        String dbStitching = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbStitching = "ephsit_extended_data_stitch";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbStitching = "ephuat_extended_data_stitch";
            }
        }else{
          //  dbStitching = "ephsit_extended_data_stitch";
           dbStitching = "ephuat_extended_data_stitch";
        }
        return dbStitching;
    }
}
