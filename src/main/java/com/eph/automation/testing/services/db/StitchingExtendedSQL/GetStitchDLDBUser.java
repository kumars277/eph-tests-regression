package com.eph.automation.testing.services.db.StitchingExtendedSQL;

public class GetStitchDLDBUser {

    public static String getProdExtDB(){
        String dbProdExt = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdExt = "product_ext_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdExt = "product_ext_database_uat";
            }
             else{
                dbProdExt = "product_ext_database_uat";
            }


        }else{
           dbProdExt = "product_ext_database_sit";
         //   dbProdExt = "product_ext_database_uat";
        }
        return dbProdExt;
    }

    public static String getProdStagingDB(){
        String dbProdStg = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdStg = "product_staging_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdStg = "product_staging_database_uat";
            }
            else{
                dbProdStg = "product_staging_database_uat";
            }
        }else{
            dbProdStg = "product_staging_database_sit";
            //   dbProdExt = "product_ext_database_uat";
        }
        return dbProdStg;
    }

    public static String getStitchingExtdb(){
        String dbExtStitching = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbExtStitching = "ephsit_extended_data_stitch";
            }
            else{
                dbExtStitching = "ephuat_extended_data_stitch";
            }
        }else{
            dbExtStitching = "ephsit_extended_data_stitch";
         //  dbExtStitching = "ephuat_extended_data_stitch";
        }
        return dbExtStitching;
    }
}
