package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLAccessObject;
import java.util.List;



@StaticInjection

public class JRBIAccessDLContext {
    public static List<JRBIDLAccessObject> recordsFromDataFullLoad;
    public static List<JRBIDLAccessObject> recordsFromFromCurrentWork;
}
