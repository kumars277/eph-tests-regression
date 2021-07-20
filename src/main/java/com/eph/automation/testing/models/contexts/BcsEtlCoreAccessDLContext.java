package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.bcs.BcsEtlCoreDLAccessObject;
import java.util.List;


@StaticInjection

public class BcsEtlCoreAccessDLContext {

    private BcsEtlCoreAccessDLContext(){
        Log.info("Sonar Lint Fix");
    }

    public static List<BcsEtlCoreDLAccessObject> recordsFromInboundData;
    public static List<BcsEtlCoreDLAccessObject> recordsFromCurrent;
    public static List<BcsEtlCoreDLAccessObject> recFromCurrentHist;
    public static List<BcsEtlCoreDLAccessObject> recFromTransformFile;
    public static List<BcsEtlCoreDLAccessObject> recFromDiffOfTransformFile;
    public static List<BcsEtlCoreDLAccessObject> recFromDeltaCurrent;
    public static List<BcsEtlCoreDLAccessObject> recFromDeltaCurrentHist;
    public static List<BcsEtlCoreDLAccessObject> recFromDiffOfDeltaAndCurrHist;
    public static List<BcsEtlCoreDLAccessObject> recFromExclDelta;
    public static List<BcsEtlCoreDLAccessObject> recFromDiffOfDeltaAndExcl;
    public static List<BcsEtlCoreDLAccessObject> recFromLatest;



}

