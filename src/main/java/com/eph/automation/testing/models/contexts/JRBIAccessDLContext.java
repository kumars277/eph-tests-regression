package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLManifestationAccessObject;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLWorkAccessObject;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLPersonAccessObject;

import java.util.List;



@StaticInjection

public class JRBIAccessDLContext {
    public static List<JRBIDLWorkAccessObject> recordsFromDataFullLoad;
    public static List<JRBIDLWorkAccessObject> recordsFromFromCurrentWork;
    public static List<JRBIDLWorkAccessObject> recordsFromFromCurrentWorkHistory;
    public static List<JRBIDLWorkAccessObject> recordsFromFromPreviousWorkHistory;
    public static List<JRBIDLWorkAccessObject> recordsFromFromPreviousWork;
    public static List<JRBIDLWorkAccessObject> recordsFromFromDeltaWork;
    public static List<JRBIDLWorkAccessObject> recordsFromFromDeltaWorkHistory;
    public static List<JRBIDLWorkAccessObject> recordsFromDiffDeltaAndWorkHistory;
    public static List<JRBIDLWorkAccessObject> recordsFromExcludeWork;

    public static List<JRBIDLManifestationAccessObject> recordsFromDataFullLoadManif;
    public static List<JRBIDLManifestationAccessObject> recordsFromFromCurrentManif;
    public static List<JRBIDLManifestationAccessObject> recordsFromFromCurrentManifHistory;
    public static List<JRBIDLManifestationAccessObject> recordsFromFromPreviousManif;
    public static List<JRBIDLManifestationAccessObject> recordsFromFromPreviousManifHistory;
    public static List<JRBIDLManifestationAccessObject> recordsFromFromDeltaManifHistory;
    public static List<JRBIDLManifestationAccessObject> recordsFromFromDeltaManif;
    public static List<JRBIDLManifestationAccessObject> recordsFromDiffDeltaAndManifHistory;
    public static List<JRBIDLManifestationAccessObject> recordsFromExcludeManif;


    public static List<JRBIDLPersonAccessObject>recordsFromDataFullLoadPerson;
    public static List<JRBIDLPersonAccessObject> recordsFromFromCurrentPerson;
    public static List<JRBIDLPersonAccessObject> recordsFromFromCurrentPersonHistory;
    public static List<JRBIDLPersonAccessObject> recordsFromFromPreviousPerson;
    public static List<JRBIDLPersonAccessObject> recordsFromFromPreviousPersonHistory;
    public static List<JRBIDLPersonAccessObject> recordsFromFromDeltaPersonHistory;
    public static List<JRBIDLPersonAccessObject> recordsFromFromDeltaPerson;
    public static List<JRBIDLPersonAccessObject> recordsFromDiffDeltaAndPersonHistory;
    public static List<JRBIDLPersonAccessObject> recordsFromExcludePerson;

}

