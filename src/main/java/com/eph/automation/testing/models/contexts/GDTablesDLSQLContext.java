package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.GDTableDLSQL.GDTableDLSQLObject;
import com.eph.automation.testing.models.dao.consumerApp.r12Objects;

import java.util.List;


@StaticInjection

public class GDTablesDLSQLContext {
    public static List<GDTableDLSQLObject>recordsFromSql;
    public static List<GDTableDLSQLObject>recordsFromDL;
    public static List<GDTableDLSQLObject> recordsFromGDTableIdentifiers;
    public static List<GDTableDLSQLObject> recordsFromCrossRefIdentifier;
    public static List<r12Objects>recsR12source;
    public static List<r12Objects>recsR12target;
    public static List<r12Objects>sourceEPHMercury;
    public static List<r12Objects>mercuryPrintView;

}

