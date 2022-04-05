package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.GDTableDLSQL.GDTableDLSQLObject;

import java.util.List;


@StaticInjection

public class GDTablesDLSQLContext {
    public static List<GDTableDLSQLObject>recordsFromSql;
    public static List<GDTableDLSQLObject>recordsFromDL;

}

