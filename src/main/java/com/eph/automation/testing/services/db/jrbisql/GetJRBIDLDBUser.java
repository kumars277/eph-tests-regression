package com.eph.automation.testing.services.db.jrbisql;

public class GetJRBIDLDBUser {

    private GetJRBIDLDBUser() {
        throw new IllegalStateException("Utility class");
    }

    public static String getJRBIDataBase(){
        String dbJRBIDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJRBIDL = "jrbi_staging_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
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
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdDb = "product_staging_database_uat";
            }
        }else{
          dbProdDb = "product_staging_database_sit";
        }
        return dbProdDb;
    }

    public static String getProductExtdb(){
        String dbProdExtdb = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdExtdb = "product_ext_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdExtdb = "product_ext_database_uat";
            }
         }else{
           dbProdExtdb = "product_ext_database_sit";
        }
        return dbProdExtdb;
    }
}
