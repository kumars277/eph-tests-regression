package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.BCSDataLake.BCSCurrentTableDataObject;
import com.eph.automation.testing.models.dao.BCSDataLake.BCSHistoryTableDataObject;
import com.eph.automation.testing.models.dao.BCSDataLake.BCSInitialIngestDataObject;
import com.eph.automation.testing.models.dao.BCSDataLake.BCSPreviousTableDataObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@StaticInjection

public class BCSDataQualityContext {
    public static List<BCSInitialIngestDataObject> bcsInitialIngestDataObjectList;
    public static List<BCSCurrentTableDataObject> bcsCurrentTableDataObjectList;
    //public static List<BCSPreviousTableDataObject> bcsPreviousTableDataObjectsList;
    public static List<BCSHistoryTableDataObject> bcsHistoryTableDataObjectsList;
}
