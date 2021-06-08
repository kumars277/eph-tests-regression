package com.eph.automation.testing.models.contexts.PromisETL;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.PROMISDataLake.PRMTablesCurrentObject;
import com.eph.automation.testing.models.dao.PROMISDataLake.PRMTablesETLObject;
import com.eph.automation.testing.models.dao.PROMISDataLake.PRMTablesInboundObject;

import java.util.List;


@StaticInjection

public class PromisContext {
    public static List<PRMTablesInboundObject> tbPRMDataObjectsFromInbound;
    public static List<PRMTablesCurrentObject> tbPRMDataObjectsFromCurrent;
    public static List<PRMTablesETLObject> tbPRMDataObjectsFromDeltaQuery;
    public static List<PRMTablesETLObject> tbPRMDataObjectsFromtransformFile;
    public static List<PRMTablesETLObject> tbPRMDataObjectsFromDelta;
    public static List<PRMTablesETLObject> tbPRMDataObjectsFromHistoryExcludingQuery;
    public static List<PRMTablesETLObject> tbPRMDataObjectsFromHistoryExcluding;
    public static List<PRMTablesETLObject> tbPRMDataObjectsFromLatest;
    public static List<PRMTablesETLObject> tbPRMDataObjectsFromLatestQuery;
    public static List<PRMTablesETLObject> tbPRMDataObjectsFromTransformMapping;
    public static List<PRMTablesETLObject> tbPRMDataObjectsFromTransformMappingCurrent;
    public static List<PRMTablesETLObject> tbPRMDataObjectsFromDL;
    }
