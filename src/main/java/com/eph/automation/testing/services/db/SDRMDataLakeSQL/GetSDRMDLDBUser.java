package com.eph.automation.testing.services.db.SDRMDataLakeSQL;

public class GetSDRMDLDBUser {

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
          //  dbSDDL = "sdbooks_staging_uat";
        }
        return dbSDRM;
    }

    public static String getProdStagingDataBase(){

        String dbProdstgDb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdstgDb = "product_staging_database_sit";
            }
            else{
                dbProdstgDb = "product_staging_database_uat";
            }
        }else{
            dbProdstgDb = "product_staging_database_sit";
            //  dbProdDb = "product_staging_database_uat";
        }
        return dbProdstgDb;
    }


    public static String getProdExtDataBase(){

        String dbProdstgDb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdstgDb = "product_ext_database_sit";
            }
            else{
                dbProdstgDb = "product_ext_database_uat";
            }
        }else{
            dbProdstgDb = "product_ext_database_sit";
            //  dbProdDb = "product_staging_database_uat";
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
            dbProdDb = "product_database_sit";
            //  dbProdDb = "product_staging_database_uat";
        }
        return dbProdDb;
    }


}


