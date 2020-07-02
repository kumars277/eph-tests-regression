package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.EPHDataLake.*;
import java.util.List;
/**
 * Created by Thomas Kruck on 15/02/2020
 */
@StaticInjection
public class LovTablesDLContext {
    public static List<LovTableDLObject> LovTableDataObjectsFromEPH;
    public static List<LovTableDLObject> LovTableDataObjectsFromDL;
}
