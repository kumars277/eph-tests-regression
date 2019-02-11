package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.GenericDataObject;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.eph.automation.testing.models.dao.ManifestationIdentifierObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
//import com.eph.automation.testing.models.dao.ProductWorkEntity;

import java.util.List;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
@StaticInjection
public class DataQualityContext {

    public static List<GenericDataObject> genericDataObjectsFromSource;
    public static List<WorkDataObject> workDataObjectsFromSource;
    public static List<WorkDataObject> workDataObjectsFromPMXSTG;
    public static List<WorkDataObject> workDataObjectsFromEPH;
    public static List<WorkDataObject> workDataObjectsFromEPHGD;

    public static List<ManifestationDataObject> manifestationDataObjectsFromEPH;
    public static List<ManifestationDataObject> manifestationDataObjectsFromPMX;
    public static List<ManifestationDataObject> manifestationDataObjectsFromEPHSA;
    public static List<ManifestationDataObject> manifestationDataObjectsFromEPHGD;


    public static List<ManifestationIdentifierObject> manifestationIdentifiersDataObjectsFromSA;
    public static List<ManifestationIdentifierObject> manifestationIdentifiersDataObjectsFromGD;


    //public static List<ProductWorkEntity> productEntityObjectsFromSource;
    //public static List<ProductWorkEntity> productEntityObjectsFromEPH;
    public static String productIdentifierID;
    public static String productIdFromStg;
    public static String productIdentifierISSN_ID;

}
