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
            default:dbBCSDL = "sit";break;
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
       // Log.info("bcs DL environment : "+dbBCSDL);
        return dbBCSDL;
    }

}
