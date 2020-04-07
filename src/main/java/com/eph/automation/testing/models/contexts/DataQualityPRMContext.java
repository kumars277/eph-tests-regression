package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.PROMISDataLake.PRMTablesDLObject;

import java.util.List;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
@StaticInjection


public class DataQualityPRMContext {

    public static List<PRMTablesDLObject> tbPRMDataObjectsFromOracle;
    public static List<PRMTablesDLObject> tbPRMDataObjectsFromDL;

}
