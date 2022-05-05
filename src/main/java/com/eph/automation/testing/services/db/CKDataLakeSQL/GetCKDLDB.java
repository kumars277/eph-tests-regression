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
            dbCKDL = "dpp_staging_sit";
            //dbCKDL = "dpp_staging_uat";
        }
        return dbCKDL;
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
            dbCKCMMSDL = "eph_supplemental_sit";
            //dbCKDL = "eph_supplemental_uat";
        }
        return dbCKCMMSDL;
    }
}
