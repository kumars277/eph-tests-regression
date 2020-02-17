package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.datalake.*;


import java.util.List;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
@StaticInjection
public class DataQualityDLContext {

    public static List<WorkDataDLObject> tbWorkDataObjectsFromEPH;
    public static List<WorkDataDLObject> tbWorkDataObjectsFromDL;

    public static List<ProductDataDLObject> tbProductDataObjectsFromEPH;
    public static List<ProductDataDLObject> tbProductDataObjectsFromDL;

    public static List<ManifestationDataDLObject> tbManifestationDataObjectsFromEPH;
    public static List<ManifestationDataDLObject> tbManifestationDataObjectsFromDL;

    public static List<PersonDataDLObject> tbPersonDataObjectsFromEPH;
    public static List<PersonDataDLObject> tbPersonDataObjectsFromDL;

}
