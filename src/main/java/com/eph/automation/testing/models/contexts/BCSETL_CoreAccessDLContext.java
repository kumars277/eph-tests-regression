package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.BCS.BCS_ETLCoreDLAccessObject;

import java.util.List;


@StaticInjection

public class BCSETL_CoreAccessDLContext {
    public static List<BCS_ETLCoreDLAccessObject> recordsFromInboundData;
    public static List<BCS_ETLCoreDLAccessObject> recordsFromCurrent;
    public static List<BCS_ETLCoreDLAccessObject> recFromCurrentHist;
    public static List<BCS_ETLCoreDLAccessObject> recFromTransformFile;
    public static List<BCS_ETLCoreDLAccessObject> recFromDiffOfTransformFile;
    public static List<BCS_ETLCoreDLAccessObject>recFromDeltaCurrent;
    public static List<BCS_ETLCoreDLAccessObject>recFromDeltaCurrentHist;
    public static List<BCS_ETLCoreDLAccessObject> recFromDiffOfDeltaAndCurrHist;
    public static List<BCS_ETLCoreDLAccessObject>recFromExclDelta;
    public static List<BCS_ETLCoreDLAccessObject> recFromDiffOfPersonDeltaAndCurrHist;
    public static List<BCS_ETLCoreDLAccessObject>recFromPersonExclDelta;
    public static List<BCS_ETLCoreDLAccessObject> recFromDiffOfDeltaAndExcl;
    public static List<BCS_ETLCoreDLAccessObject> recFromLatest;



}

