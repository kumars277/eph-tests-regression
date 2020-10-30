package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.BCS_ETLCore.BCS_ETLCoreDLAccessObject;

import java.util.List;


@StaticInjection

public class BCSETL_CoreAccessDLContext {
    public static List<BCS_ETLCoreDLAccessObject> recordsFromInboundData;
    public static List<BCS_ETLCoreDLAccessObject> recordsFromCurrent;
    public static List<BCS_ETLCoreDLAccessObject> recordsFromPersonCurrent;
    public static List<BCS_ETLCoreDLAccessObject> recFromCurrentHist;
    public static List<BCS_ETLCoreDLAccessObject> recFromPersonCurrentHist;
    public static List<BCS_ETLCoreDLAccessObject> recFromPersonTransFile;
    public static List<BCS_ETLCoreDLAccessObject> recFromTransformFile;
    public static List<BCS_ETLCoreDLAccessObject> recFromDiffOfTransformFile;
    public static List<BCS_ETLCoreDLAccessObject>recFromDeltaCurrent;
    public static List<BCS_ETLCoreDLAccessObject> recFromPersonDiffOfTransformFile;
    public static List<BCS_ETLCoreDLAccessObject>recFromPersonDeltaCurrent;



}

