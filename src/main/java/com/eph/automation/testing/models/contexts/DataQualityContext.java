package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.*;
//import com.eph.automation.testing.models.dao.ProductDataObject;

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

    public static List<ProductDataObject> productDataObjectsFromPMX;
    public static List<ProductDataObject> productDataObjectsFromEPHSTG;
    public static List<ProductDataObject> productDataObjectsFromEPHSA;
    public static List<ProductDataObject> productDataObjectsFromEPHGD;

    public static List<ProductRelationshipDataObject> productRelationshipDataObjectsFromPMX;
    public static List<ProductRelationshipDataObject> productRelationshipDataObjectsFromEPHSTG;
    public static List<ProductRelationshipDataObject> productRelationshipDataObjectsFromEPHSA;
    public static List<ProductRelationshipDataObject> productRelationshipDataObjectsFromEPHGD;

    public static List<ManifestationIdentifierObject> manifestationIdentifiersDataObjectsFromSA;
    public static List<ManifestationIdentifierObject> manifestationIdentifiersDataObjectsFromGD;

    //public static List<ProductDataObject> productEntityObjectsFromSource;
    //public static List<ProductDataObject> productEntityObjectsFromEPH;
    public static String productIdentifierID;
    public static String productIdFromStg;
    public static String productIdentifierISSN_ID;

}
