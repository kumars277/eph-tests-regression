package com.eph.automation.testing.models.contexts;
import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.EPHDataLake.DPPTables.DPPTablesDLObject;

import java.util.List;

/**
 * Created by Thomas Kruck on 12/03/2020
 */
@StaticInjection
public class DPPTablesDLContext {
    public static List<DPPTablesDLObject> DPPTableDataObjectsFromEPH;
    public static List<DPPTablesDLObject> DPPTableDataObjectsFromDL;
}
