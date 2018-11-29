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
    public static List<ProductDataObject> productDetailsFromPMXSource;
    public static List<ProductDataObject> productDetailsFromSDLayer;
    public static String loadId;
    public static String sourceId;
    public static String talendJobName;

    public static String TALEND_PROJECT_008_NAME = "PRJ_008_CUSTOMERINSIGHT_ETL";

    public static int totalOrgsInSourceFile;
    public static int totalOrgsFilteredByTalend;

    public static List<String> listOfOrgSourceIdsFromSourceFile;

    public static String sourceName;



}
