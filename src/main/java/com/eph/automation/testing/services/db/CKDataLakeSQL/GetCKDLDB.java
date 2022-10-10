package com.eph.automation.testing.services.db.CKDataLakeSQL;

public class GetCKDLDB {
    public static String getCKDataBase() {
        String dbCKDL = null;
        if (System.getProperty("ENV") != null) {
            if (System.getProperty("ENV").equalsIgnoreCase("SIT")) {
                dbCKDL = "dpp_staging_sit";
            } else if (System.getProperty("ENV").equalsIgnoreCase("SIT2")) {
                dbCKDL = "dpp_staging_sit2";
            } else if (System.getProperty("ENV").equalsIgnoreCase("UAT")) {
                dbCKDL = "dpp_staging_uat";
            } else if (System.getProperty("ENV").equalsIgnoreCase("UAT2")) {
                dbCKDL = "dpp_staging_uat2";
            }
        } else {
//            dbCKDL = "dpp_staging_sit";
            dbCKDL = "dpp_staging_uat";
        }
        return dbCKDL;
    }
    public static String getProductDataBase() {
        String ProductDb = null;
        if (System.getProperty("ENV") != null) {
            if (System.getProperty("ENV").equalsIgnoreCase("SIT")) {
                ProductDb = "product_database_sit";
            } else if (System.getProperty("ENV").equalsIgnoreCase("SIT2")) {
                ProductDb = "product_database_sit";
            } else if (System.getProperty("ENV").equalsIgnoreCase("UAT")) {
                ProductDb = "product_database_uat";
            } else if (System.getProperty("ENV").equalsIgnoreCase("UAT2")) {
                ProductDb = "product_database_uat";
            }
        } else {
//            ProductDb = "product_database_sit";
            ProductDb = "product_database_uat";
        }
        return ProductDb;
    }
    public static String getCKCMMSDataBase() {
        String dbCKCMMSDL = null;
        if (System.getProperty("ENV") != null) {
            if (System.getProperty("ENV").equalsIgnoreCase("SIT")) {
                dbCKCMMSDL = "eph_supplemental_sit";
            } else if (System.getProperty("ENV").equalsIgnoreCase("SIT2")) {
                dbCKCMMSDL = "eph_supplemental_sit2";
            } else if (System.getProperty("ENV").equalsIgnoreCase("UAT")) {
                dbCKCMMSDL = "eph_supplemental_uat";
            } else if (System.getProperty("ENV").equalsIgnoreCase("UAT2")) {
                dbCKCMMSDL = "eph_supplemental_uat2";
            }
        } else {
//            dbCKCMMSDL = "eph_supplemental_sit";
            dbCKCMMSDL = "eph_supplemental_uat";
        }
        return dbCKCMMSDL;
    }

    public static String getCKEBTDDataBase() {
        String dbCKCMMSDL = null;
        if (System.getProperty("ENV") != null) {
            if (System.getProperty("ENV").equalsIgnoreCase("SIT")) {
                dbCKCMMSDL = "ebtd_staging_sit";
            } else if (System.getProperty("ENV").equalsIgnoreCase("SIT2")) {
                dbCKCMMSDL = "ebtd_staging_sit2";
            } else if (System.getProperty("ENV").equalsIgnoreCase("UAT")) {
                dbCKCMMSDL = "ebtd_staging_uat";
            } else if (System.getProperty("ENV").equalsIgnoreCase("UAT2")) {
                dbCKCMMSDL = "ebtd_staging_uat2";
            }
        } else {
//            dbCKCMMSDL = "ebtd_staging_sit";
            dbCKCMMSDL = "ebtd_staging_uat";
        }
        return dbCKCMMSDL;
    }
}
