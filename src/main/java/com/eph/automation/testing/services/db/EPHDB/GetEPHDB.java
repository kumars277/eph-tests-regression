package com.eph.automation.testing.services.db.EPHDB;

public class GetEPHDB {
    public static String getStitchingDB(){
        String dbStitch  = null;
        if (System.getProperty("ENV") != null){
            if(System.getProperty("ENV").equalsIgnoreCase("SIT")){
                dbStitch = "ephsit_extended_data_stitch";
            }
            else{
                dbStitch = "ephuat_extended_data_stitch";
            }

        }else{
            dbStitch = "ephsit_extended_data_stitch";
        }
        return dbStitch;
    }
}
