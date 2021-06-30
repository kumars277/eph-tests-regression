package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLAccessObject;
import java.util.List;



@StaticInjection
public class JRBIAccessDLContext {
    public static List<JRBIDLAccessObject> recordsFromDataFullLoad;
    public static List<JRBIDLAccessObject> recordsFromSource;
    public static List<JRBIDLAccessObject> recordsFromTarget;
    public static List<JRBIDLAccessObject> recordsFromDiffCurrentAndPrevious;
    public static List<JRBIDLAccessObject> recordsFromDiffDeltaAndHistory;
    public static List<JRBIDLAccessObject> recordsFromAddDeltaCurrAndExclude;
    public static List<JRBIDLAccessObject> recordsFromLAtest;


    public static List<JRBIDLAccessObject> recordsFromFromDeltaWorkHistory;





    public static List<JRBIDLAccessObject> recordsFromFromDeltaPersonHistory;

      public static List<JRBIDLAccessObject> recordsFromExcel;


}

