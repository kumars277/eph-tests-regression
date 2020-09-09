package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.SDBooksDataLake.SDBooksDLAccessObject;

import java.util.List;


@StaticInjection

public class SDAccessDLContext {
    public static List<SDBooksDLAccessObject> recordsFromInboundData;
    public static List<SDBooksDLAccessObject> recordsFromCurrentUrl;
    public static List<SDBooksDLAccessObject> recordsFromCurrentUrlHistory;
    public static List<SDBooksDLAccessObject> recordsFromPreviousUrlHistory;
    public static List<SDBooksDLAccessObject> recordsFromPreviousUrl;
    public static List<SDBooksDLAccessObject> recordsFromDeltaCurrentUrl;
    public static List<SDBooksDLAccessObject> recordsFromDeltaUrlHistory;
    public static List<SDBooksDLAccessObject> recordsFromDiffDeltaAndCurrentHistoryUrl;
    public static List<SDBooksDLAccessObject> recordsFromExcludeUrl;
    public static List<SDBooksDLAccessObject> recordsFromLAtestUrl;
    public static List<SDBooksDLAccessObject> recordsFromAddDeltaAndExcludeUrl;
    public static List<SDBooksDLAccessObject> recordsFromDiffCurrentAndPreviousUrl;

}

