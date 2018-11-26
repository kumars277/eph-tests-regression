package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.GenericDataObject;
import com.eph.automation.testing.services.db.DataAccessServiceImpl;
import com.eph.automation.testing.services.db.sql.DataQualityChecksSQL;
import cucumber.api.java.en.Given;

/**
 * Created by RAVIVARMANS on 25/11/2018.
 */
public class DataQualityCheckSteps {

    @StaticInjection
    public DataQualityContext dataQualityContext;

    private static String sql;


    @Given("^I am EPH System User$")
    public void I_am_System_User() throws Throwable {
        sql = DataQualityChecksSQL.GET_TOTAL_COUNT_BY_TABLE_NAME
                .replace("SCHEMA_PARAM", Constants.PPM_PROD_SCHEMA)
                .replace("TABLE_PARAM", "AP_LOCATION");

        dataQualityContext.genericDataObjectsFromSource = DataAccessServiceImpl.getPPMTableRows(sql, GenericDataObject.class);

        System.out.println(dataQualityContext.genericDataObjectsFromSource.get(0).TOTAL_COUNT);

    }
}
