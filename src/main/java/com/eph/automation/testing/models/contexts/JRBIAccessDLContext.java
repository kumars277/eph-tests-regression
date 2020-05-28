package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLManifestationAccessObject;
import com.eph.automation.testing.models.dao.JRBIDataLakeAccess.JRBIDLWorkAccessObject;
import java.util.List;



@StaticInjection

public class JRBIAccessDLContext {
    public static List<JRBIDLWorkAccessObject> recordsFromDataFullLoad;
    public static List<JRBIDLWorkAccessObject> recordsFromFromCurrentWork;

    public static List<JRBIDLManifestationAccessObject> recordsFromDataFullLoadManif;
    public static List<JRBIDLManifestationAccessObject> recordsFromFromCurrentManif;
}
