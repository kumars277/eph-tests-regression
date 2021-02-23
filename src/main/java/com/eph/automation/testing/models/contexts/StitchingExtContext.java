package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.StitchingExtended.*;

import java.util.List;


@StaticInjection

public class StitchingExtContext {

//    public static List<JRBIDLWorkAccessObject> recordsFromExtendeWork;
//    public static JRBIWorkExtJsonObject  recordsFromWorkStitching;
//   // public static List<JRBIDLPersonAccessObject>JRBIWorkExtJson;

    public static List<ManifestationExtAccessObject> recordsFromManifExtended;
    public static List<ManifestationExtAccessObject> recordsFromManifExtPageCount;
    public static ManifExtJsonObject  recordsFromManifStitching;
    public static WorkExtJsonObject  recordsFromWorkStitching;
    public static List<ManifestationExtAccessObject> recFromManifStitchExtended;
    public static List<WorkExtAccessObject> recFromWorkTypeStitchExtended;

    public static List<WorkExtAccessObject> recordsFromWorkExtended;



//    public static List<JRBIDLPersonAccessObject> recordsFromPersonExtended;

}

