package com.eph.automation.testing.services.db.JMDataLakeSQL;

public class GetJMDLDBUser {

    public static String[] getJMDataBase(){
        String dbJMSQL  = null;
        String dbJMDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJMDL = "journalmaestro_staging_sit";
                dbJMSQL = "jmf_sit_application";
            }
            else{
                dbJMDL = "journalmaestro_staging_uat";
                dbJMSQL = "jmf_sit_application";
            }

        }else{
            dbJMDL = "journalmaestro_staging_uat";
            dbJMSQL = "jmf_sit_application";
        }
        return new String[]{dbJMSQL,dbJMDL};
    }

    public static String getJMDB(){
        String dbJMDataLake = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJMDataLake = "journalmaestro_sit";
            }
            else{
                dbJMDataLake = "journalmaestro_uat";
            }

        }else{
            dbJMDataLake = "journalmaestro_sit";
        }
        return dbJMDataLake;
    }

    public static String getJMDB2(){
        String dbJMDataLake = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJMDataLake = "journalmaestro_staging_sit2";
            }
            else{
                dbJMDataLake = "journalmaestro_staging_uat2";
            }

        }else{
            dbJMDataLake = "journalmaestro_staging_sit2";
        }
        return dbJMDataLake;
    }

    public static String getProdDataBase(){
        String dbProdDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdDL = "product_database_sit";
            }
            else{
                dbProdDL = "product_database_uat";
            }

        }else{
            dbProdDL = "product_database_sit";
        }
        return dbProdDL;
    }

    public static String getProdStagingDataBase(){
        String dbStageProdDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbStageProdDL = "product_staging_database_sit";
            }
            else{
                dbStageProdDL = "product_staging_database_uat";
            }

        }else{
            dbStageProdDL = "product_staging_database_sit";
        }
        return dbStageProdDL;
    }

    public static String getProdStagingDataBase2(){
        String dbStageProdDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbStageProdDL = "product_staging_database_sit2";
            }
            else{
                dbStageProdDL = "product_staging_database_uat2";
            }

        }else{
            dbStageProdDL = "product_staging_database_uat2";
        }
        return dbStageProdDL;
    }
}
