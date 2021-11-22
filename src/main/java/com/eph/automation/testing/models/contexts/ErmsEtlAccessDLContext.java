package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.erms.ErmsDLAccessObject;

import java.util.List;


@StaticInjection

public class ErmsEtlAccessDLContext {

    private ErmsEtlAccessDLContext(){
        Log.info("Sonar Lint Fix");
    }

    public static List<ErmsDLAccessObject> recordsFromInboundData;
    public static List<ErmsDLAccessObject> recordsFromCurrent;
    public static List<ErmsDLAccessObject> recFromCurrentHist;
    public static List<ErmsDLAccessObject> recFromTransformFile;
    public static List<ErmsDLAccessObject> recFromDiffOfTransformFile;
    public static List<ErmsDLAccessObject> recFromDeltaCurrent;
    public static List<ErmsDLAccessObject> recFromDeltaCurrentHist;
    public static List<ErmsDLAccessObject> recFromDiffOfDeltaAndCurrHist;
    public static List<ErmsDLAccessObject> recFromExclDelta;
    public static List<ErmsDLAccessObject> recFromDiffOfDeltaAndExcl;
    public static List<ErmsDLAccessObject> recFromLatest;



}

