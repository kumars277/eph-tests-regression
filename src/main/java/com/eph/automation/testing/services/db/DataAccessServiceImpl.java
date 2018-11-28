package com.eph.automation.testing.services.db;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;

import java.util.List;

/**
 * Created by RAVIVARMANS on 26/11/2018.
 */
public class DataAccessServiceImpl {

    public static List getPPMTableRows(String sql,Class klass) {
        return DBManager.getDBResultAsBeanList(sql, klass, Constants.PPM_PROD_URL);
    }

}
