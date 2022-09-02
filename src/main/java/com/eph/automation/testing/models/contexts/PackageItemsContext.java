package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.PkgItemsAccessObject;

import java.util.List;

@StaticInjection

public class PackageItemsContext {

    private PackageItemsContext(){
        Log.info("Sonar Lint Fix");
    }

    public static List<PkgItemsAccessObject> recordsFromSemarchyData;
    public static List<PkgItemsAccessObject> recordsFromPackageItem;

}
