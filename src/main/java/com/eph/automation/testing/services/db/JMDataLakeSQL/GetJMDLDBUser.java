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
}
