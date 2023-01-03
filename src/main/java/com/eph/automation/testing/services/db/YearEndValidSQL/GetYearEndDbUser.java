package com.eph.automation.testing.services.db.YearEndValidSQL;
import com.eph.automation.testing.helper.Log;

public class GetYearEndDbUser {

    private GetYearEndDbUser(){
        Log.info("Year End Validation DB");}

    public static String getYearEndValidDB(){
        String dbYearEnd = null;
        if (System.getProperty("ENV") != null)
        {
            switch (System.getProperty("ENV")) {
                case "SIT" : dbYearEnd = "eph_year_end_database_sit"; break;
                case "UAT" : dbYearEnd = "eph_year_end_database_uat"; break;
                default    : dbYearEnd = "eph_year_end_database_sit";break;
            }
        }
        else{dbYearEnd = "eph_year_end_database_sit";}
        return dbYearEnd;
    }

}


