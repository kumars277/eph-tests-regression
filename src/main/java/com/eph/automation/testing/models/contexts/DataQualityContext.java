package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.api.AvailabilityExtendedTestClass;
import com.eph.automation.testing.models.api.ManifestationExtendedTestClass;
import com.eph.automation.testing.models.api.WorkExtendedTestClass;
import com.eph.automation.testing.models.dao.*;
import com.eph.automation.testing.services.db.sql.WorkRelationshipDataObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
@StaticInjection
public class DataQualityContext {

    public static List<GenericDataObject> genericDataObjectsFromSource;
    public static List<WorkDataObject> workDataObjectsFromSource;
    public static List<WorkDataObject> workDataObjectsFromPMXSTG;
    public static List<WorkDataObject> workDataObjectsFromSTGDQ;
    public static List<WorkDataObject> workDataObjectsFromEPH;
    public static List<WorkDataObject> workDataObjectsFromEPHGD;
    public static List<WorkDataObject> workDataObjectsAccProd;

    public static List<ManifestationDataObject> manifestationDataObjectsFromEPHSTG;
    public static List<ManifestationDataObject> manifestationDataObjectsFromPMX;
    public static List<ManifestationDataObject> manifestationDataObjectsFromEPHSA;
    public static List<ManifestationDataObject> manifestationDataObjectsFromEPHDQ;
    public static List<ManifestationDataObject> manifestationDataObjectsFromEPHGD;

    public static List<ProductDataObject> productDataObjectsFromPMX;
    public static List<ProductDataObject> productDataObjectsFromEPHSTG;
    public static List<ProductDataObject> productDataObjectsFromEPHSTGDQ;
    public static List<ProductDataObject> productDataObjectsFromEPHSA;
    public static List<ProductDataObject> productDataObjectsFromEPHGD;

    public static List<ProductRelationshipDataObject> productRelationshipDataObjectsFromPMX;
    public static List<ProductRelationshipDataObject> productRelationshipDataObjectsFromEPHSTG;
    public static List<ProductRelationshipDataObject> productRelationshipDataObjectsFromEPHSA;
    public static List<ProductRelationshipDataObject> productRelationshipDataObjectsFromEPHGD;

    public static List<WorkRelationshipDataObject> workRelationshipParentDataObjectsFromEPGD;
    public static List<WorkRelationshipDataObject> workRelationshipChildDataObjectsFromEPGD;

    public static List<ManifestationIdentifierObject> manifestationIdentifiersDataObjectsFromSTG;
    public static List<ManifestationIdentifierObject> manifestationIdentifiersDataObjectsFromSA;
    public static List<ManifestationIdentifierObject> manifestationIdentifiersDataObjectsFromGD;


    public static List<PersonDataObject> personDataObjectsFromPMX;
    public static List<PersonDataObject> personDataObjectsFromEPHSTG;
    public static List<PersonDataObject> personDataObjectsFromEPHDQ;
    public static List<PersonDataObject> personDataObjectsFromEPHSA;
    public static List<PersonDataObject> personDataObjectsFromEPHGD;

    public static List<PersonProductRoleDataObject> personProductRoleDataObjectsFromEPHSTGDQ;
    public static List<PersonProductRoleDataObject> personProductRoleDataObjectsFromEPHSTG;
    public static List<PersonProductRoleDataObject> personProductRoleDataObjectsFromEPHSA;
    public static List<PersonProductRoleDataObject> personProductRoleDataObjectsFromEPHGD;

    public static List<PersonWorkRoleDataObject> personWorkRoleDataObjectsFromPMX;
    public static List<PersonWorkRoleDataObject> personWorkRoleDataObjectsFromEPHSTG;
    public static List<PersonWorkRoleDataObject> personWorkRoleDataObjectsFromEPHSA;
    public static List<PersonWorkRoleDataObject> personWorkRoleDataObjectsFromEPHGD;

    public static List<AccountableProductDataObject> accountableProductDataObjectsFromPMX;
    public static List<AccountableProductDataObject> accountableProductDataObjectsFromSTG;
    public static List<AccountableProductDataObject> accountableProductDataObjectsFromSTGDQ;
    public static List<AccountableProductDataObject> accountableProductDataObjectsFromSA;
    public static List<AccountableProductDataObject> accountableProductDataObjectsFromGD;

    public static List<SubjectAreaDataObject> subjectAreaDataObjectsFromPMX;
    public static List<SubjectAreaDataObject> subjectAreaDataObjectsFromSTG;
    public static List<SubjectAreaDataObject> subjectAreaDataObjectsFromSA;
    public static List<SubjectAreaDataObject> subjectAreaDataObjectsFromGD;

    public static List<WorkSubjectAreaLinkDataObject> workSubjectAreaDataObjectsFromPMX;
    public static List<WorkSubjectAreaLinkDataObject> workSubjectAreaDataObjectsFromSTG;
    public static List<WorkSubjectAreaLinkDataObject> workSubjectAreaDataObjectsFromSA;
    public static List<WorkSubjectAreaLinkDataObject> workSubjectAreaDataObjectsFromGD;

    public static List<String> ids;
    public static List<Map<?, ?>> randomIdsData;

    //public static List<ProductDataObject> productEntityObjectsFromSource;
    //public static List<ProductDataObject> productEntityObjectsFromEPH;
    public static String productIdentifierID;
    public static String productIdFromStg;
    public static String productIdentifierISSN_ID;

    public static WorkExtendedTestClass workExtendedTestClass;
    public static ManifestationExtendedTestClass manifestationExtendedTestClass;
    public static AvailabilityExtendedTestClass availabilityExtendedTestClass;

    public static String uiUnderTest;
    public static Properties prop_filters = new Properties();
    public static ArrayList<String> RowData;
    public static ArrayList<String> DataToWrite = new ArrayList<>();



}
