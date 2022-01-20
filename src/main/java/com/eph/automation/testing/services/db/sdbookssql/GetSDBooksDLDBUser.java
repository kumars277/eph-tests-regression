package com.eph.automation.testing.services.db.sdbookssql;

public class GetSDBooksDLDBUser {

    private GetSDBooksDLDBUser() {
        throw new IllegalStateException("Utility class");
    }

    public static String getSDDataBase(){
        String dbSDDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbSDDL = "sdbooks_staging_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbSDDL = "sdbooks_staging_uat";
            }
        }else{
            dbSDDL = "sdbooks_staging_sit";
        }
        return dbSDDL;
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

}
