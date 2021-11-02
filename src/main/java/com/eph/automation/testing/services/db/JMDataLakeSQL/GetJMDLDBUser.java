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
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbJMDL = "journalmaestro_staging_uat";
                dbJMSQL = "jmf_uat_application";
            }

        }else{
             dbJMDL = "journalmaestro_staging_uat";
            dbJMSQL = "jmf_uat_application";
        }
        return new String[]{dbJMSQL,dbJMDL};
    }

    public static String getJMDB(){
        String dbJMDataLake = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJMDataLake = "journalmaestro_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbJMDataLake = "journalmaestro_uat2";
            }
        }else{
            dbJMDataLake = "journalmaestro_uat2";
        }
        return dbJMDataLake;
    }

    public static String getJMDB2(){
        String dbJMDataLake = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJMDataLake = "journalmaestro_staging_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbJMDataLake = "journalmaestro_staging_uat";
            }
        }else{
           dbJMDataLake = "journalmaestro_staging_uat";
        }
        return dbJMDataLake;
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
            dbProdDL = "product_database_uat";
           // dbProdDL = "product_database_sit";
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
           dbStageProdDL = "product_staging_database_uat";
        }
        return dbStageProdDL;
    }
}
