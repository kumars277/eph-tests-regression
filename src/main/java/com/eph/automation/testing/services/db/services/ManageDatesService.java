package com.eph.automation.testing.services.db.services;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ManageDatesService {

    public static String oldDate(){
        DateFormat dateFormat = new SimpleDateFormat("dd/MMM/yy");
        Date date = new Date(System.currentTimeMillis() - 48*60*60*1000);
        String oldDate = dateFormat.format(date);

        return oldDate;
    }

    public static String newDate(){
        DateFormat dateFormat = new SimpleDateFormat("dd/MMM/yy");
        Date date = new Date(System.currentTimeMillis() - 24*60*60*1000);
        String newDate = dateFormat.format(date);

        return newDate;
    }

    public static String currentDate(){
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        Date date = new Date(System.currentTimeMillis());
        String currentDate = dateFormat.format(date);

        return currentDate;
    }
}
