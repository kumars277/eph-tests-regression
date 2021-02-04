package com.eph.automation.testing.services.db.BCS_ETLExtendedSQL;

public class GetBCS_ETLExtendedDLDBUser {

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
            //dbBCS = "bcs_ingestion_database_uat";
        }
        return dbBCS;
    }

    public static String getDL_CoreViewDataBase(){
        String dbProd = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProd = "product_staging_database_sit";
            }
            else{
                dbProd = "product_staging_database_uat";
            }

        }else{
            dbProd = "product_staging_database_sit";
             // dbProd = "bcs_ingestion_database_uat";
        }
        return dbProd;
    }

    public static String getProdDataBase(){
        String dbProdDB = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdDB = "product_database_sit";
            }
            else{
                dbProdDB = "product_database_uat";
            }

        }else{
            dbProdDB = "product_database_sit";
             // dbProdDB = "bcs_ingestion_database_uat";
        }
        return dbProdDB;
    }

    public static String getProductStagingDatabase(){
        String dbProdStgDb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdStgDb = "product_staging_database_sit";
            }
            else{
                dbProdStgDb = "product_staging_database_uat";
            }
        }else{
            dbProdStgDb = "product_staging_database_sit";
            //  dbProdDb = "product_staging_database_uat";
        }
        return dbProdStgDb;
    }

    public static String getJM_CoreDataBase(){
        String dbJM = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJM = "journalmaestro_sit";
            }
            else{
                dbJM = "journalmaestro_uat";
            }

        }else{
            dbJM = "journalmaestro_sit";
            //  dbJM = "bcs_ingestion_database_uat";
        }
        return dbJM;
    }

    public static String getSDRMDataBase(){
        String dbSDRM = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbSDRM = "sdrm_staging_sit";
            }
            else{
                dbSDRM = "sdrm_staging_uat";
            }

        }else{
            dbSDRM = "sdrm_staging_sit";
            //  dbSDRM = "sdrm_staging_uat";
        }
        return dbSDRM;
    }

    public static String getSDBooksDataBase(){
        String dbSDBooks = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbSDBooks = "sdbooks_staging_sit";
            }
            else{
                dbSDBooks = "sdbooks_staging_uat";
            }

        }else{
            dbSDBooks = "sdbooks_staging_sit";
            //  dbSDBooks = "sdbooks_staging_uat";
        }
        return dbSDBooks;
    }

    public static String getPromisDataBase(){
        String dbPRM = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbPRM = "promis_staging_sit";
            }
            else{
                dbPRM = "promis_staging_uat";
            }

        }else{
            dbPRM = "promis_staging_sit";
            //  dbPRM = "promis_staging_uat";
        }
        return dbPRM;
    }

    public static String getDL_ExtViewDataBase(){
        String dbProdExt = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdExt = "product_ext_database_sit";
            }
            else{
                dbProdExt = "product_ext_database_uat";
            }

        }else{
            dbProdExt = "product_ext_database_sit";
            // dbProd = "bcs_ingestion_database_uat2";
        }
        return dbProdExt;
    }

    public static String getJRBIDataBase(){
        String dbJRBI = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJRBI = "jrbi_staging_sit";
            }
            else{
                dbJRBI = "jrbi_staging_uat";
            }

        }else{
            dbJRBI = "jrbi_staging_sit";
            //  dbJRBI = "jrbi_staging_sit";
        }
        return dbJRBI;
    }




}
