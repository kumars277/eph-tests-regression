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
        //     dbJMDL = "journalmaestro_staging_uat";
         //   dbJMSQL = "jmf_uat_application";

            dbJMDL = "journalmaestro_staging_sit";
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
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbJMDataLake = "journalmaestro_uat";
            }
        }else{
           // dbJMDataLake = "journalmaestro_uat";
            dbJMDataLake = "journalmaestro_sit";
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
          // dbJMDataLake = "journalmaestro_staging_uat";
            dbJMDataLake = "journalmaestro_staging_sit";
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
            //dbProdDL = "product_database_uat";
            dbProdDL = "product_database_sit";
        }
        return dbProdDL;
    }

    public static String getProdPresentationDB(){
        String dbPresentation = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbPresentation = "product_core_database_v4_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbPresentation = "product_core_database_v4_uat";
            }
        }else{
            //dbProdDL = "product_database_uat";
            dbPresentation = "product_core_database_v4_sit";
        }
        return dbPresentation;
    }

    public static String getCrossRefDb(){
        String crossRefDb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                crossRefDb = "eph_sit_crossreference_database";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                crossRefDb = "eph_uat_crossreference_database";
            }
        }else{
            //dbProdDL = "product_database_uat";
            crossRefDb = "eph_sit_crossreference_database";
        }
        return crossRefDb;
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
           //dbStageProdDL = "product_staging_database_uat";
            dbStageProdDL = "product_staging_database_sit";
        }
        return dbStageProdDL;
    }
}
