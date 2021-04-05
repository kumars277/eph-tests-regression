package com.eph.automation.testing.services.db.PROMISDataLakeSQL;

public class GetDLCrossRefDB {
    public static String getDLCrossRefDB(){
        String CrossrefDB = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                CrossrefDB = "eph_sit_crossreference_database.eph_identifier_cross_reference_v";
            }
            else{
                CrossrefDB = "eph_uat_crossreference_database.eph_identifier_cross_reference_v";
            }

        }else{
            CrossrefDB = "eph_sit_crossreference_database.eph_identifier_cross_reference_v";
        }
        return CrossrefDB;
    }
}
