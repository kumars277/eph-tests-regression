package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.ProductDataObject;

import java.util.List;

/**
 * Created by RAVIVARMANS on 03/09/2018.
 */
@StaticInjection
public class DataLoadContext {

    public static String incrementalFile;
    public static String sftpLandingLocation;
    public static List<ProductDataObject> productDetailsFromPMXSource;
    public static List<ProductDataObject> productDetailsFromSDLayer;
    public static String loadId;
    public static String sourceId;
    public static String talendJobName;

    public static int totalOrgsInSourceFile;
    public static int totalOrgsFilteredByTalend;

    public static List<String> listOfOrgSourceIdsFromSourceFile;

    public static String sourceName;

    //This is the Project Name in the MY SQL Ref Key for each Talend Project
    public static String TALEND_PROJECT_008_NAME = "PRJ_008_CUSTOMERINSIGHT_ETL";
    public static String TALEND_RINGGOLD_PROJECT_114_NAME = "PRJ_114_RG_TO_CUSTOMER_HUB";



}
