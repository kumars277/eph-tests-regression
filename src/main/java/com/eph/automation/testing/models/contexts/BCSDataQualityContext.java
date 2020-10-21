package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.BCSDataLake.BCSCurrentTableDataObject;
import com.eph.automation.testing.models.dao.BCSDataLake.BCSInitialIngestDataObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@StaticInjection

public class BCSDataQualityContext {

    public static List<BCSInitialIngestDataObject> bcsInitialIngestDataObjectList;
    public static List<BCSCurrentTableDataObject> bcsCurrentTableDataObjectList;
}
