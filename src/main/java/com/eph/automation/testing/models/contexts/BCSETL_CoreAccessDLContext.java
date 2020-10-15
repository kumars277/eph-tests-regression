package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.BCS_ETLCore.BCS_ETLCoreDLAccessObject;

import java.util.List;


@StaticInjection

public class BCSETL_CoreAccessDLContext {
    public static List<BCS_ETLCoreDLAccessObject> recordsFromInboundData;
    public static List<BCS_ETLCoreDLAccessObject> recordsFromCurrent;




}

