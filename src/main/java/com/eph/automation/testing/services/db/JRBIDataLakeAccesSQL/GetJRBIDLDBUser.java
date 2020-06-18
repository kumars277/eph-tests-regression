package com.eph.automation.testing.services.db.JRBIDataLakeAccesSQL;

public class GetJRBIDLDBUser {

    public static String getJRBIDataBase(){
        String dbJRBIDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJRBIDL = "jrbi_staging_sit";
            }
            else{
                dbJRBIDL = "jrbi_staging_uat";
            }

        }else{
            dbJRBIDL = "jrbi_staging_sit";
        }
        return dbJRBIDL;
    }

    public static String getProductDatabase(){
        String dbProdDb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdDb = "product_staging_database_sit";
            }
            else{
                dbProdDb = "product_staging_database_uat";
            }

        }else{
            dbProdDb = "product_staging_database_sit";
        }
        return dbProdDb;

    }
}
