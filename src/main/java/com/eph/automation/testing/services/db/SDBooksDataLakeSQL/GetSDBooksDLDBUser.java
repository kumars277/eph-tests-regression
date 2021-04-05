package com.eph.automation.testing.services.db.SDBooksDataLakeSQL;

public class GetSDBooksDLDBUser {

    public static String getSDDataBase(){
        String dbSDDL = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbSDDL = "sdbooks_staging_sit";
            }
            else{
                dbSDDL = "sdbooks_staging_uat";
            }

        }else{
            dbSDDL = "sdbooks_staging_sit";
          //  dbSDDL = "sdbooks_staging_uat";
        }
        return dbSDDL;
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
          //  dbProdDb = "product_staging_database_uat";
        }
        return dbProdDb;
    }

}
