package com.eph.automation.testing.models.contexts.JMETL;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.JMDataLake.JM_ETLExtendedDLAccessObject;

import java.util.List;


@StaticInjection

public class JMETL_ExtendedAccessDLContext {
    public static List<JM_ETLExtendedDLAccessObject>recordsFromSource;
    public static List<JM_ETLExtendedDLAccessObject>recordsFromJMFulFilment;


}

