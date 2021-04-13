package com.eph.automation.testing.models.contexts.JMETL;
import com.eph.automation.testing.models.dao.JMDataLake.JMETLObject;
import com.eph.automation.testing.models.dao.JMDataLake.JsonObjects.StitchManifestationObjectJson;
import com.eph.automation.testing.models.dao.JMDataLake.STITCHObject;

import java.util.List;

public class JMContext {
    public static List<JMETLObject> JMObjectsFromDL;
    public static List<JMETLObject> JMTransformObjectsFromDL;
    public static STITCHObject  StitchingWorkCoreObject;
    public static StitchManifestationObjectJson StitchingManifestationObject;
}
