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
    public static List<ProductDataObject> productDataObjectsFromEPHSTGCan;
    public static List<ProductDataObject> productDataObjectsFromEPHSA;
    public static List<ProductDataObject> productDataObjectsFromEPHGD;

    public static List<ProductRelationshipDataObject> productRelationshipDataObjectsFromPMX;
    public static List<ProductRelationshipDataObject> productRelationshipDataObjectsFromEPHSTG;
    public static List<ProductRelationshipDataObject> productRelationshipDataObjectsFromEPHSA;
    public static List<ProductRelationshipDataObject> productRelationshipDataObjectsFromEPHGD;

    public static List<ManifestationIdentifierObject> manifestationIdentifiersDataObjectsFromSA;
    public static List<ManifestationIdentifierObject> manifestationIdentifiersDataObjectsFromGD;

    public static List<PersonDataObject> personDataObjectsFromPMX;
    public static List<PersonDataObject> personDataObjectsFromEPHSTG;
    public static List<PersonDataObject> personDataObjectsFromEPHSA;
    public static List<PersonDataObject> personDataObjectsFromEPHGD;

    public static List<PersonProductRoleDataObject> personProductRoleDataObjectsFromEPHSTGCAN;
    public static List<PersonProductRoleDataObject> personProductRoleDataObjectsFromEPHSTG;
    public static List<PersonProductRoleDataObject> personProductRoleDataObjectsFromEPHSA;
    public static List<PersonProductRoleDataObject> personProductRoleDataObjectsFromEPHGD;

    public static List<PersonWorkRoleDataObject> personWorkRoleDataObjectsFromPMX;
    public static List<PersonWorkRoleDataObject> personWorkRoleDataObjectsFromEPHSTG;
    public static List<PersonWorkRoleDataObject> personWorkRoleDataObjectsFromEPHSA;
    public static List<PersonWorkRoleDataObject> personWorkRoleDataObjectsFromEPHGD;

    //public static List<ProductDataObject> productEntityObjectsFromSource;
    //public static List<ProductDataObject> productEntityObjectsFromEPH;
    public static String productIdentifierID;
    public static String productIdFromStg;
    public static String productIdentifierISSN_ID;



}
