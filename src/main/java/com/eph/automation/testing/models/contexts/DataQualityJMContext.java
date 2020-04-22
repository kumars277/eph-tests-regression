package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.JMDataLake.JMTablesDLObject;


import java.util.List;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
@StaticInjection


public class DataQualityJMContext {

    public static List<JMTablesDLObject> tbJMDataObjectsFromMysql;
    public static List<JMTablesDLObject> tbJMDataObjectsFromDL;

}
