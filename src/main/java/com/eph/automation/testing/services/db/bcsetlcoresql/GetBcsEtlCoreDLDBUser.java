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
            case "UAT" : dbBCS = "bcs_ingestion_database_uat"; break;
            default    : dbBCS = "bcs_ingestion_database_sit";break;
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
                    case "UAT":
                        dbProd = "product_staging_database_uat";
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
        if (System.getProperty("ENV") != null)
        {
            switch (System.getProperty("ENV")) {
            //updated by Nishant @ 03 Mar 2022
            case "SIT" : dbERMS = "erms_staging_sit"; break;
            case "UAT" : dbERMS = "erms_staging_uat"; break;
            default    : dbERMS = "erms_staging_sit";break;
        }
        }
        else{dbERMS = "erms_staging_sit";}
        return dbERMS;
    }

    public static String getProdDataBase(){
        String dbProdDB = null;
        if (System.getProperty("ENV") != null)
        {
        switch (System.getProperty("ENV")) {
            //updated by Nishant @ 03 Mar 2022
            case "SIT" : dbProdDB = "product_database_sit"; break;
            case "UAT" : dbProdDB = "product_database_uat"; break;
            default    : dbProdDB = "product_database_sit";break;
        }
        }
        else{dbProdDB = "product_database_sit";}
        return dbProdDB;
    }

    public static String getProdDataBaseGd(){
        String dbProdDb = null;
        if (System.getProperty("ENV") != null)
        {
        switch (System.getProperty("ENV")) {
            //updated by Nishant @ 03 Mar 2022
            case "SIT":
                dbProdDb = "product_database_sit";
                break;
            case "UAT":
                dbProdDb = "product_database_uat";
                break;
            default:
                dbProdDb = "product_database_sit";
                break;

        }}else{
            dbProdDb = "product_database_sit";
        }
        return dbProdDb;
    }

    public static String getdppDataBase(){
        String dbDpp = null;
        if (System.getProperty("ENV") != null)
        {
        switch (System.getProperty("ENV")) {
            //updated by Nishant @ 03 Mar 2022
            case "SIT" : dbDpp = "dpp_database_sit"; break;
             case "UAT" : dbDpp = "dpp_database_uat"; break;
            default    : dbDpp = "dpp_database_sit"; break;
        }}else{
             dbDpp = "dpp_database_sit";
        }
        return dbDpp;
    }

     public static String getJmCoreDataBase(){
        String dbJM = null;
         if (System.getProperty("ENV") != null)
         {
        switch (System.getProperty("ENV")) {
            //updated by Nishant @ 03 Mar 2022
            case "SIT" : dbJM = "journalmaestro_sit"; break;
            case "UAT" : dbJM = "journalmaestro_uat"; break;
            default    : dbJM = "journalmaestro_sit"; break;
        }}else{
             dbJM = "journalmaestro_sit";
         }
        return dbJM;
    }
}
