package com.eph.automation.testing.services.db.bcsetlextendedsql;

public class GetBcsEtlExtendedDLDBUser {
    private GetBcsEtlExtendedDLDBUser(){
        throw new IllegalStateException("Utility class");
    }

    public static String getBcsEtlCoreDataBase(){
        String dbBCS = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbBCS = "bcs_ingestion_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbBCS = "bcs_ingestion_database_uat";
            }

        }else{
            //dbBCS = "bcs_ingestion_database_uat";
            dbBCS = "bcs_ingestion_database_sit";
        }
        return dbBCS;
    }

    public static String getDL_CoreViewDataBase(){
        String dbProd = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProd = "product_staging_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProd = "product_staging_database_uat";
            }
        }else{
             //dbProd = "product_staging_database_uat";
            dbProd = "product_staging_database_sit";
        }
        return dbProd;
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
           // dbProdDB = "product_database_uat";
            dbProdDB = "product_database_sit";
        }
        return dbProdDB;
    }

    public static String getProductStagingDatabase(){
        String dbProdStgDb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdStgDb = "product_staging_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdStgDb = "product_staging_database_uat";
            }
        }else{
           // dbProdStgDb = "product_staging_database_uat";
            dbProdStgDb = "product_staging_database_sit";
        }
        return dbProdStgDb;
    }

    public static String getJM_CoreDataBase(){
        String dbJM = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJM = "journalmaestro_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbJM = "journalmaestro_uat";
            }

        }else{
            //dbJM = "journalmaestro_uat";
            dbJM = "journalmaestro_sit";
        }
        return dbJM;
    }

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
           //  dbSDRM = "sdrm_staging_uat";
            dbSDRM = "sdrm_staging_sit";
        }
        return dbSDRM;
    }

    public static String getSDBooksDataBase(){
        String dbSDBooks = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbSDBooks = "sdbooks_staging_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbSDBooks = "sdbooks_staging_uat";
            }
        }else{
             // dbSDBooks = "sdbooks_staging_uat";
            dbSDBooks = "sdbooks_staging_sit";
        }
        return dbSDBooks;
    }

    public static String getPromisDataBase(){
        String dbPRM = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbPRM = "promis_staging_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbPRM = "promis_staging_uat";
            }
        }else{
         //   dbPRM = "promis_staging_uat";
            dbPRM = "promis_staging_sit";
        }
        return dbPRM;
    }

    public static String getDL_ExtViewDataBase(){
        String dbProdExt = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdExt = "product_ext_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdExt = "product_ext_database_uat";
            }
        }else{
         //  dbProdExt = "product_ext_database_uat";
            dbProdExt = "product_ext_database_sit";
        }
        return dbProdExt;
    }

    public static String getJRBIDataBase(){
        String dbJRBI = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJRBI = "jrbi_staging_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbJRBI = "jrbi_staging_uat";
            }
        }else{
           // dbJRBI = "jrbi_staging_uat";
            dbJRBI = "jrbi_staging_sit";
        }
        return dbJRBI;
    }




}
