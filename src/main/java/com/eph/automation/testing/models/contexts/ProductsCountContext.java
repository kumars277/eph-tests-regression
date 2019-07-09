package com.eph.automation.testing.models.contexts;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.dao.ProductCountObject;

import java.util.List;

@StaticInjection
public class ProductsCountContext {
    public static List<ProductCountObject> productCountPMX;
    public static List<ProductCountObject> productCountStg;
    public static List<ProductCountObject> productCountStgCan;
    public static List<ProductCountObject> productCountStgCanDQ;
    public static List<ProductCountObject> productCountStgBooks;
    public static List<ProductCountObject> productCountStgSubscription;
    public static List<ProductCountObject> productCountStgBulk;
    public static List<ProductCountObject> productCountStgReprints;
    public static List<ProductCountObject> productCountStgBack;
    public static List<ProductCountObject> productCountStgOA;
    public static List<ProductCountObject> productCountStgOAMore;
    public static List<ProductCountObject> productCountStgAC;
    public static List<ProductCountObject> productCountStgACMore;
    public static List<ProductCountObject> productCountStgPackages;
    public static List<ProductCountObject> productCountEPHSA;
    public static List<ProductCountObject> productCountEPHGD;
    public static List<ProductCountObject> productCountEPHAE;
    public static List<ProductCountObject> productCountStgFromPMX;
}
