package com.eph.automation.testing.services.db.BCS_ETLCoreSQL;

public class GetBCS_ETLCoreDLDBUser {

    public static String getBCS_ETLCoreDataBase(){
        String dbBCS = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbBCS = "bcs_ingestion_database_sit2";
            }
            else{
                dbBCS = "bcs_ingestion_database_uat";
            }

        }else{
            dbBCS = "bcs_ingestion_database_sit2";
          //  dbSDDL = "bcs_ingestion_database_uat";
        }
        return dbBCS;
    }

    public static String getDL_CoreViewDataBase(){
        String dbProd = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProd = "product_staging_database_sit2";
            }
            else{
                dbProd = "product_staging_database_uat";
            }

        }else{
            dbProd = "product_staging_database_sit2";
            //  dbSDDL = "bcs_ingestion_database_uat";
        }
        return dbProd;
    }

    public static String getProdDataBase(){
        String dbProdDB = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdDB = "product_database_sit2";
            }
            else{
                dbProdDB = "product_database_uat";
            }

        }else{
            dbProdDB = "product_database_sit";
            //  dbSDDL = "bcs_ingestion_database_uat";
        }
        return dbProdDB;
    }

    public static String getJM_CoreDataBase(){
        String dbJM = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJM = "journalmaestro_sit2";
            }
            else{
                dbJM = "journalmaestro_uat";
            }

        }else{
            dbJM = "journalmaestro_sit2";
            //  dbSDDL = "bcs_ingestion_database_uat";
        }
        return dbJM;
    }


}
