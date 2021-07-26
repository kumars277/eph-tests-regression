package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.bcs.BcsEtlExtendedDLAccessObject;

import java.util.List;


@StaticInjection

public class BCSETL_ExtendedAccessDLContext {
    public static List<BcsEtlExtendedDLAccessObject>   recordsFromInboundData;
    public static List<BcsEtlExtendedDLAccessObject>   recordsFromCurrent;
    public static List<BcsEtlExtendedDLAccessObject>   recFromCurrentHist;
    public static List<BcsEtlExtendedDLAccessObject>   recFromTransformFile;
    public static List<BcsEtlExtendedDLAccessObject>   recFromDiffOfTransformFile;
    public static List<BcsEtlExtendedDLAccessObject>   recFromDeltaCurrent;
    public static List<BcsEtlExtendedDLAccessObject>   recFromDeltaCurrentHist;
    public static List<BcsEtlExtendedDLAccessObject>   recFromDiffOfDeltaAndCurrHist;
    public static List<BcsEtlExtendedDLAccessObject>   recFromExclDelta;
     public static List<BcsEtlExtendedDLAccessObject>  recFromSumOfDeltaAndExcl;
    public static List<BcsEtlExtendedDLAccessObject>   recFromLatest;


}

