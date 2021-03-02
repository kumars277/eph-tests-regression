package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.SDRMDataLake.SDRMExtendedAccessObject;

import java.util.List;

@StaticInjection
public class SDRMProductExtendedDLContext {



    public static List<SDRMExtendedAccessObject> recordsFromSDRMLatestProductData;

    public static List<SDRMExtendedAccessObject> recordsFromProductAvailabilityExtendedAllSourceData;

    public static List<SDRMExtendedAccessObject> recordsFromProductExtendedAvailabilityData;

}
