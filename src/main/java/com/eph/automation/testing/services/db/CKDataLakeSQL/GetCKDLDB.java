package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class GetCKDLDB {
    public static String getCKDataBase() {
        String dbCKDL = null;
        if (System.getProperty("ENV") != null) {
            if (System.getProperty("ENV").equalsIgnoreCase("SIT")) {
                dbCKDL = "dpp_staging_sit";
            } else if (System.getProperty("ENV").equalsIgnoreCase("UAT")) {
                dbCKDL = "dpp_staging_uat";
            }
        } else {
            dbCKDL = "dpp_staging_sit";
        }
        return dbCKDL;
    }
    public static String getProductDataBase() {
        String ProductDb = null;
        if (System.getProperty("ENV") != null) {
            if (System.getProperty("ENV").equalsIgnoreCase("SIT")) {
                ProductDb = "product_database_sit";
            }else if (System.getProperty("ENV").equalsIgnoreCase("UAT")) {
                ProductDb = "product_database_uat";
            }
        } else {
            ProductDb = "product_database_sit";
        }
        return ProductDb;
    }
    public static String getCKCMMSDataBase() {
        String dbCKCMMSDL = null;
        if (System.getProperty("ENV") != null) {
            if (System.getProperty("ENV").equalsIgnoreCase("SIT")) {
                dbCKCMMSDL = "eph_supplemental_sit";
            } else if (System.getProperty("ENV").equalsIgnoreCase("UAT")) {
                dbCKCMMSDL = "eph_supplemental_uat";
            }
        } else {
            dbCKCMMSDL = "eph_supplemental_sit";
        }
        return dbCKCMMSDL;
    }

    public static String getCKEBTDDataBase() {
        String dbCKCMMSDL = null;
        if (System.getProperty("ENV") != null) {
            if (System.getProperty("ENV").equalsIgnoreCase("SIT")) {
                dbCKCMMSDL = "ebtd_staging_sit";
            }else if (System.getProperty("ENV").equalsIgnoreCase("UAT")) {
                dbCKCMMSDL = "ebtd_staging_uat";
            }
        } else {
           dbCKCMMSDL = "ebtd_staging_sit";
        }
        return dbCKCMMSDL;
    }
}
