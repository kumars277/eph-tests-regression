package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLAccessObject;
import java.util.List;



@StaticInjection
public class JRBIAccessDLContext {
    public static List<JRBIDLAccessObject> recordsFromDataFullLoad;
    public static List<JRBIDLAccessObject> recordsFromFromCurrentWork;
    public static List<JRBIDLAccessObject> recordsFromFromCurrentWorkHistory;
    public static List<JRBIDLAccessObject> recordsFromFromPreviousWorkHistory;
    public static List<JRBIDLAccessObject> recordsFromFromPreviousWork;
    public static List<JRBIDLAccessObject> recordsFromFromDeltaWork;
    public static List<JRBIDLAccessObject> recordsFromFromDeltaWorkHistory;
    public static List<JRBIDLAccessObject> recordsFromDiffDeltaAndWorkHistory;
    public static List<JRBIDLAccessObject> recordsFromExcludeWork;
    public static List<JRBIDLAccessObject> recordsFromLAtestWork;
    public static List<JRBIDLAccessObject> recordsFromAddDeltaAndWorkExclude;
    public static List<JRBIDLAccessObject> recordsFromDiffCurrentAndPreviousWork;

    public static List<JRBIDLAccessObject> recordsFromDataFullLoadManif;
    public static List<JRBIDLAccessObject> recordsFromFromCurrentManif;
    public static List<JRBIDLAccessObject> recordsFromFromCurrentManifHistory;
    public static List<JRBIDLAccessObject> recordsFromFromPreviousManif;
    public static List<JRBIDLAccessObject> recordsFromFromPreviousManifHistory;
    public static List<JRBIDLAccessObject> recordsFromFromDeltaManifHistory;
    public static List<JRBIDLAccessObject> recordsFromFromDeltaManif;
    public static List<JRBIDLAccessObject> recordsFromDiffDeltaAndManifHistory;
    public static List<JRBIDLAccessObject> recordsFromExcludeManif;
    public static List<JRBIDLAccessObject> recordsFromLAtestManif;
    public static List<JRBIDLAccessObject> recordsFromAddDeltaAndManifExclude;
    public static List<JRBIDLAccessObject> recordsFromDiffCurrentAndPreviousManif;

    public static List<JRBIDLAccessObject>recordsFromDataFullLoadPerson;
    public static List<JRBIDLAccessObject> recordsFromFromCurrentPerson;
    public static List<JRBIDLAccessObject> recordsFromFromCurrentPersonHistory;
    public static List<JRBIDLAccessObject> recordsFromFromPreviousPerson;
    public static List<JRBIDLAccessObject> recordsFromFromPreviousPersonHistory;
    public static List<JRBIDLAccessObject> recordsFromFromDeltaPersonHistory;
    public static List<JRBIDLAccessObject> recordsFromFromDeltaPerson;
    public static List<JRBIDLAccessObject> recordsFromDiffDeltaAndPersonHistory;
    public static List<JRBIDLAccessObject> recordsFromExcludePerson;
    public static List<JRBIDLAccessObject> recordsFromLAtestPerson;
    public static List<JRBIDLAccessObject> recordsFromAddDeltaAndPersonExclude;
    public static List<JRBIDLAccessObject> recordsFromDiffCurrentAndPreviousPerson;
    public static List<JRBIDLAccessObject> recordsFromExcel;


}

