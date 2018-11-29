package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.Product;

/**
 * Created by RAVIVARMANS on 28/11/2018.
 */
@StaticInjection
public class LoadBatchContext {

    public static String batchId;
    public static String oldBatchId;
    public static String loadId;
    public static String eventId;
    public static String oldLooadId;
    public static boolean jobStatus;

    public static Product productDetailsToCreate;
}
