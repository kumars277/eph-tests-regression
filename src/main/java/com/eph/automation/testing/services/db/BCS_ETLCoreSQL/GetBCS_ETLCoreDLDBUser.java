package com.eph.automation.testing.services.db.BCS_ETLCoreSQL;

public class GetBCS_ETLCoreDLDBUser {

    public static String getBCS_ETLCoreDataBase(){
        String dbBCS = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbBCS = "bcs_ingestion_database_sit2";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbBCS = "bcs_ingestion_database_uat";
            }else{
                dbBCS = "bcs_ingestion_database_uat2";
            }

        }else{
            dbBCS = "bcs_ingestion_database_sit2";
            //dbBCS = "bcs_ingestion_database_uat2";
        }
        return dbBCS;
    }

    public static String getDL_CoreViewDataBase(){
        String dbProd = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProd = "product_staging_database_sit2";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProd = "product_staging_database_uat";
            }else{
                dbProd = "product_staging_database_uat2";
            }

        }else{
            dbProd = "product_staging_database_sit2";
           // dbProd = "product_staging_database_uat2";
        }
        return dbProd;
    }

    public static String getProdDataBase(){
        String dbProdDB = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdDB = "product_database_sit2";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdDB = "product_database_uat";
            }else{
                dbProdDB = "product_database_uat2";
            }

        }else{
            dbProdDB = "product_database_sit2";
              //dbProdDB = "product_database_uat2";
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
            }else{
                dbDpp = "dpp_database_uat2";
            }

        }else{
            dbDpp = "dpp_database_sit";
           //  dbDpp = "dpp_database_uat";
        }
        return dbDpp;
    }

    public static String getProductStagingDatabase(){
        String dbProdStgDb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdStgDb = "product_staging_database_sit2";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdStgDb = "product_staging_database_uat";
            }else{
                dbProdStgDb = "product_staging_database_uat2";
            }
        }else{
            dbProdStgDb = "product_staging_database_sit2";
            //dbProdStgDb = "product_staging_database_uat2";
        }
        return dbProdStgDb;
    }

    public static String getJM_CoreDataBase(){
        String dbJM = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJM = "journalmaestro_sit2";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbJM = "journalmaestro_uat";
            }else{
                dbJM = "journalmaestro_uat2";
            }

        }else{
            dbJM = "journalmaestro_sit2";
            //  dbJM = "journalmaestro_uat2";
        }
        return dbJM;
    }

    public static String getSDRMDataBase(){
        String dbSDRM = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbSDRM = "sdrm_staging_sit2";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbSDRM = "sdrm_staging_uat";
            }else{
                dbSDRM = "sdrm_staging_uat2";
            }

        }else{
           dbSDRM = "sdrm_staging_sit2";
            //  dbSDRM = "sdrm_staging_uat2";
        }
        return dbSDRM;
    }

    public static String getSDBooksDataBase(){
        String dbSDBooks = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbSDBooks = "sdbooks_staging_sit2";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbSDBooks = "sdbooks_staging_uat";
            }else{
                dbSDBooks = "sdbooks_staging_uat2";
            }

        }else{
           dbSDBooks = "sdbooks_staging_sit2";
             // dbSDBooks = "sdbooks_staging_uat2";
        }
        return dbSDBooks;
    }

    public static String getPromisDataBase(){
        String dbPRM = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbPRM = "promis_staging_sit2";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbPRM = "promis_staging_uat";
            }else{
                dbPRM = "promis_staging_uat2";
            }

        }else{
            dbPRM = "promis_staging_sit2";
            //  dbPRM = "promis_staging_uat2";
        }
        return dbPRM;
    }

    public static String getDL_ExtViewDataBase(){
        String dbProdExt = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdExt = "product_ext_database_sit2";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdExt = "product_ext_database_uat";
            }else{
                dbProdExt = "product_ext_database_uat2";
            }

        }else{
            dbProdExt = "product_ext_database_sit2";
           // dbProdExt = "product_ext_database_uat2";
        }
        return dbProdExt;
    }

    public static String getJRBIDataBase(){
        String dbJRBI = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJRBI = "jrbi_staging_sit2";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbJRBI = "jrbi_staging_uat";
            }else{
                dbJRBI = "jrbi_staging_uat2";
            }

        }else{
            dbJRBI = "jrbi_staging_sit2";
            //  dbJRBI = "jrbi_staging_uat2";
        }
        return dbJRBI;
    }




}
