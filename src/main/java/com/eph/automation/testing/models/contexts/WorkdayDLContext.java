package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.workDayObjects.workDayDLAccessObject;

import java.util.List;

@StaticInjection

public class WorkdayDLContext {

    private WorkdayDLContext(){
        Log.info("Sonar Lint Fix");
    }

    public static List<workDayDLAccessObject> recordsFromInboundData;
    public static List<workDayDLAccessObject> recordsFromWorkDayRefData;

}
