package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.StitchingExtended.*;

import java.util.List;


@StaticInjection

public class StitchingExtContext {

    public static List<ManifestationExtAccessObject> recordsFromManifExtended;
    public static List<ManifestationExtAccessObject> recordsFromManifExtSummary;
    public static List<ManifestationExtAccessObject> recordsFromManifExtPageCount;
    public static List<WorkExtAccessObject> recordsFromWorkExtMetric;
    public static List<WorkExtAccessObject> recordsFromWorkExtUrl;
    public static List<WorkExtAccessObject> recordsFromWorkExtSubjArea;
    public static List<WorkExtAccessObject> recordsFromWorkExtPersRole;
    public static List<WorkExtAccessObject> recordsFromWorkExtRelationSiblings;
    public static List<WorkExtAccessObject> recordsFromWorkExtEditorial;
    public static List<ManifestationExtAccessObject> recordsFromManifExtRestrict;
    public static ManifExtJsonObject  recordsFromManifStitching;
    public static ProdExtJsonObject  recordsFromProdStitching;
    public static WorkExtJsonObject  recordsFromWorkStitching;
    public static List<ManifestationExtAccessObject> recFromManifStitchExtended;

    public static List<ProductExtAccessObject> recFromProdStitchAvailExtended;
    public static List<ProductExtAccessObject> recordsFromProdExtAvail;
    public static List<ProductExtAccessObject> recordsFromProdExtPrice;
    public static List<WorkExtAccessObject> recFromWorkTypeStitchExtended;

    public static List<WorkExtAccessObject> recordsFromWorkExtended;



//    public static List<JRBIDLPersonAccessObject> recordsFromPersonExtended;

}

