package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.BCSDataLake.BCSInitialIngestDataObject;

import java.util.ArrayList;
import java.util.List;
@StaticInjection

public class BCSDataQualityContext {

    public static List<List<BCSInitialIngestDataObject>> bcsInitialIngestDataObjectList;
}
