package com.eph.automation.testing.services.db.BCSDataLakeSQL;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;

public class getBCSDataBase {




        public static String getBCSDataBase(){
            String dbBCSDL = null;

        //updated by Nishant @ 16 April 2021
            switch (TestContext.getValues().environment) {
                case "SIT":dbBCSDL = "sit";  break;
                case "UAT":dbBCSDL = "uat";  break;
                case "UAT2":dbBCSDL = "uat2";break;
                default:dbBCSDL = "uat2";break;
            }

            /*
            if (TestContext.getValues().environment != null){
                if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                    dbBCSDL = "sit";
                }
                else{
                    dbBCSDL = "uat2";
                }

            }else{
                dbBCSDL = "uat2";
                //  dbJRBIDL = "jrbi_staging_uat";
            }
            */
            Log.info("BCS DL environment : "+dbBCSDL);
            return dbBCSDL;
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

        public static String getProductGDdb(){
            String dbProdGDdb = null;
            if (System.getProperty("ENV") != null){
                if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                    dbProdGDdb = "product_database_sit";
                }
                else{
                    dbProdGDdb = "product_database_uat";
                }
            }else{
                dbProdGDdb = "product_database_sit";
                // dbProdGDdb = "product_database_uat";
            }
            return dbProdGDdb;
        }

        public static String getProductExtdb(){
            String dbProdExtdb = null;
            if (System.getProperty("ENV") != null){
                if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                    dbProdExtdb = "product_ext_database_sit";
                }
                else{
                    dbProdExtdb = "product_ext_database_uat";
                }
            }else{
                dbProdExtdb = "product_ext_database_sit";
                //  dbProdExtdb = "product_ext_database_uat";
            }
            return dbProdExtdb;
        }

        public static String getStitchingdb(){
            String dbStitching = null;
            if (System.getProperty("ENV") != null){
                if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                    dbStitching = "ephsit_extended_data_stitch";
                }
                else{
                    dbStitching = "ephuat_extended_data_stitch";
                }
            }else{
                //dbStitching = "ephsit_extended_data_stitch";
                dbStitching = "ephuat_extended_data_stitch";
            }
            return dbStitching;
        }


}
