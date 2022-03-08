package com.eph.automation.testing.steps;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

//created by Nishant @ 03 Mar 2022
public class GenericFunctions {

    public static String setRandomCount(String countOfRandomIds) {
        String numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        if(numberOfRecords==null)numberOfRecords= countOfRandomIds;
        return numberOfRecords;
    }

    public static String getDBsufix(){
        //created by Nishant @ 07 Mar 2022
        String dbsufix = null;
        switch (TestContext.getValues().environment) {
            case "SIT":dbsufix = "sit";  break;
            case "SIT2":dbsufix = "sit2";  break;
            case "UAT":dbsufix = "uat";  break;
            case "UAT2":dbsufix = "uat2";break;
            default:dbsufix = "sit";break;
        }
        return dbsufix;
    }

    public static boolean isExpired(String date) throws ParseException {
        //created by Nishant @ 07 Mar 2022
        Boolean endDated = false;
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        if(date!=null){
            Date DateToCmp = dateFormatter.parse(date);
            Date Todate = new Date();
            if(Todate.compareTo(DateToCmp)>0){    endDated = true;}
        }
        return endDated;
    }

}
