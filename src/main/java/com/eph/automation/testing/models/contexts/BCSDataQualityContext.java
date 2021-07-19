package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.bcs.BCSCurrentTableDataObject;
import com.eph.automation.testing.models.dao.bcs.BCSHistoryTableDataObject;
import com.eph.automation.testing.models.dao.bcs.BCSInitialIngestDataObject;

import java.util.List;

@StaticInjection

public class BCSDataQualityContext {
    public static List<BCSInitialIngestDataObject> bcsInitialIngestDataObjectList;
    public static List<BCSCurrentTableDataObject> bcsCurrentTableDataObjectList;
    //public static List<BCSPreviousTableDataObject> bcsPreviousTableDataObjectsList;
    public static List<BCSHistoryTableDataObject> bcsHistoryTableDataObjectsList;
}
