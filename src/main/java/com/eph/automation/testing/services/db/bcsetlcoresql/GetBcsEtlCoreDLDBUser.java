package com.eph.automation.testing.services.db.bcsetlcoresql;
//updated by Nishant @ 03 Mar 2022
import com.eph.automation.testing.helper.Log;

public class GetBcsEtlCoreDLDBUser {

    private GetBcsEtlCoreDLDBUser(){
        Log.info("implicit public");
    }

public static String getBcsETLCoreDataBase(){
    String dbBCS = null;
    if (System.getProperty("ENV") != null)
    {
        switch (System.getProperty("ENV")) {
            case "SIT" : dbBCS = "bcs_ingestion_database_sit"; break;
            case "SIT2": dbBCS = "bcs_ingestion_database_sit2";break;
            case "UAT" : dbBCS = "bcs_ingestion_database_uat"; break;
            case "UAT2": dbBCS = "bcs_ingestion_database_uat2";break;
          //  default    : dbBCS = "bcs_ingestion_database_sit";break;
        }
    }
    else{dbBCS = "bcs_ingestion_database_sit";}
    return dbBCS;
}

    public static String getDlCoreViewDataBase() {
        try {
            String dbProd = "";
            if (System.getProperty("ENV") != null) {
                switch (System.getProperty("ENV")) {
                    case "SIT":
                        dbProd = "product_staging_database_sit";
                        break;
                    case "SIT2":
                        dbProd = "product_staging_database_sit2";
                        break;
                    case "UAT":
                        dbProd = "product_staging_database_uat";
                        break;
                    case "UAT2":
                        dbProd = "product_staging_database_uat2";
                        break;
                    default:
                        dbProd = "product_staging_database_sit";
                        break;
                }
            } else {
                dbProd = "product_staging_database_sit";
            }
            return dbProd;
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }


    public static String getErmsETLCoreDataBase(){
        String dbERMS = null;


        switch (System.getProperty("ENV")) {
            //updated by Nishant @ 03 Mar 2022
            case "SIT" : dbERMS = "erms_staging_sit"; break;
            case "SIT2": dbERMS = "erms_staging_sit2";break;
            case "UAT" : dbERMS = "erms_staging_uat"; break;
            case "UAT2": dbERMS = "erms_staging_uat2";break;
            default    : dbERMS = "erms_staging_sit";break;
        }



        /*
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbERMS = "erms_staging_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbERMS = "erms_staging_uat";
            }

        }else{
            dbERMS = "erms_staging_sit";
        }*/
        return dbERMS;
    }

    public static String getProdDataBase(){
        String dbProdDB = null;
        switch (System.getProperty("ENV")) {
            //updated by Nishant @ 03 Mar 2022
            case "SIT" : dbProdDB = "product_database_sit"; break;
            case "SIT2": dbProdDB = "product_database_sit2";break;
            case "UAT" : dbProdDB = "product_database_uat"; break;
            case "UAT2": dbProdDB = "product_database_uat2";break;
            default    : dbProdDB = "product_database_sit";break;
        }
        /*
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdDB = "product_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdDB = "product_database_uat";
            }
        }else{
               dbProdDB = "product_database_sit";
        }*/
        return dbProdDB;
    }

    public static String getProdDataBaseGd(){
        String dbProdDb = null;
        switch (System.getProperty("ENV")) {
            //updated by Nishant @ 03 Mar 2022
            case "SIT" : dbProdDb = "product_database_sit"; break;
            case "SIT2": dbProdDb = "product_database_sit2";break;
            case "UAT" : dbProdDb = "product_database_uat"; break;
            case "UAT2": dbProdDb = "product_database_uat2";break;
            default    : dbProdDb = "product_database_sit"; break;
        }
        /*
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbProdDb = "product_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbProdDb = "product_database_uat";
            }

        }else{
            dbProdDb = "product_database_sit";
        }*/
        return dbProdDb;
    }

    public static String getdppDataBase(){
        String dbDpp = null;
        switch (System.getProperty("ENV")) {
            //updated by Nishant @ 03 Mar 2022
            case "SIT" : dbDpp = "dpp_database_sit"; break;
            case "SIT2": dbDpp = "dpp_database_sit2";break;
            case "UAT" : dbDpp = "dpp_database_uat"; break;
            case "UAT2": dbDpp = "dpp_database_uat2";break;
            default    : dbDpp = "dpp_database_sit"; break;
        }
        /*
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbDpp = "dpp_database_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbDpp = "dpp_database_uat";
            }
        }else{
             dbDpp = "dpp_database_sit";
        }*/
        return dbDpp;
    }

     public static String getJmCoreDataBase(){
        String dbJM = null;
        switch (System.getProperty("ENV")) {
            //updated by Nishant @ 03 Mar 2022
            case "SIT" : dbJM = "journalmaestro_sit"; break;
            case "SIT2": dbJM = "journalmaestro_sit2";break;
            case "UAT" : dbJM = "journalmaestro_uat"; break;
            case "UAT2": dbJM = "journalmaestro_uat2";break;
            default    : dbJM = "journalmaestro_sit"; break;
        }
        /*
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbJM = "journalmaestro_sit";
            }
            else if(System.getProperty("ENV").equalsIgnoreCase("UAT")){
                dbJM = "journalmaestro_uat";
            }
        }else{
              dbJM = "journalmaestro_sit";
        }*/
        return dbJM;
    }


}
