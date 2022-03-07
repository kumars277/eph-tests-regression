package com.eph.automation.testing.steps;

import com.eph.automation.testing.helper.Log;
//created by Nishant @ 03 Mar 2022
public class GenericFunctions {

    public static String setRandomCount(String countOfRandomIds)
    {
        String numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        if(numberOfRecords==null)numberOfRecords= countOfRandomIds;
     //   Log.info("numberOfRecords = " + numberOfRecords);
        return numberOfRecords;
    }
}
