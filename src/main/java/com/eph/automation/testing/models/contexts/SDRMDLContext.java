package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.SDRMDataLake.SDRMDLAccessObject;
import java.util.List;
import com.eph.automation.testing.helper.Log;

@StaticInjection

public class SDRMDLContext {

    private SDRMDLContext(){
        Log.info("Sonar Lint Fix");
    }

    public static List<SDRMDLAccessObject> recordsFromInboundData;
    public static List<SDRMDLAccessObject> recordsFromCurrentProductData;
    public static List<SDRMDLAccessObject> recordsFromProductHistoryData;
    public static List<SDRMDLAccessObject> recordsFromProductFileHistoryData;
    public static List<SDRMDLAccessObject> recordsFromDeltaCurrentProductData;
    public static List<SDRMDLAccessObject> recordsFromDeltaProductHistoryData;
    public static List<SDRMDLAccessObject> recordsFromDeltaCurrentAndDeltaHistoryData;
    public static List<SDRMDLAccessObject> recordsFromHistoryExclDeltaData;
    public static List<SDRMDLAccessObject> recordsFromDeltaCurrentAndHistoryExclDeltaData;
    public static List<SDRMDLAccessObject> recordsFromSDRMTransfromLatestProductData;
    public static List<SDRMDLAccessObject> recordsFromSDRMCurrentAndPrevFileHistoryData;

}
