package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.DL_ExtendedViews.DL_ExtendedViewsAccessObject;

import java.util.List;


@StaticInjection

public class DL_ExtViewsAccessContext {
    public static List<DL_ExtendedViewsAccessObject> recordsFromSourceIngestTable;
    public static List<DL_ExtendedViewsAccessObject> recordsFromAllExtViews;




}

