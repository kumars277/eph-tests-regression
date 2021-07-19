package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.bcs.BCS_ETLExtendedDLAccessObject;

import java.util.List;


@StaticInjection

public class BCSETL_ExtendedAccessDLContext {
    public static List<BCS_ETLExtendedDLAccessObject>   recordsFromInboundData;
    public static List<BCS_ETLExtendedDLAccessObject>   recordsFromCurrent;
    public static List<BCS_ETLExtendedDLAccessObject>   recFromCurrentHist;
    public static List<BCS_ETLExtendedDLAccessObject>   recFromTransformFile;
    public static List<BCS_ETLExtendedDLAccessObject>   recFromDiffOfTransformFile;
    public static List<BCS_ETLExtendedDLAccessObject>   recFromDeltaCurrent;
    public static List<BCS_ETLExtendedDLAccessObject>   recFromDeltaCurrentHist;
    public static List<BCS_ETLExtendedDLAccessObject>   recFromDiffOfDeltaAndCurrHist;
    public static List<BCS_ETLExtendedDLAccessObject>   recFromExclDelta;
     public static List<BCS_ETLExtendedDLAccessObject>  recFromSumOfDeltaAndExcl;
    public static List<BCS_ETLExtendedDLAccessObject>   recFromLatest;


}

