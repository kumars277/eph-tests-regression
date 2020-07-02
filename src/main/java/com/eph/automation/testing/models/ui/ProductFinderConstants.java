package com.eph.automation.testing.models.ui;

import com.eph.automation.testing.models.contexts.DataQualityContext;

public class ProductFinderConstants {

    public static String SCIENCE_ID = "@science.regn.net";
    public static String userDetail="//div[@class='pad-right']";
    public static String loginByEmail = "loginfmt";
    public static String searchBar = "//*[@id='mat-input-0']";
    public static String searchButton = "//i[@class='gm-ico-search']";
    public static String tab_product_andPackages="//div[@class='mat-tab-links']/div[2]/a[1]";
    public static String tab_manifestation="//div[@class='mat-tab-links']/div[3]/a[1]";
    public static String resultPerPage_100="//span[@class='ng-star-inserted']/a[contains(text(),'100')]";
    public static String resultPerPage_50="//span[@class='ng-star-inserted']/a[contains(text(),'50')]";
    public static String SearchResultPageCount="//div[@class='page-label']";
    public static String nextPageButton = "//div[@class='next-button']";
    public static String nextButton = "idSIButton9";
    public static String itemDetail="(//div[contains(@class,'item-detail')])";
    public static String itemDetailInner="//div[@class='ng-star-inserted']";
    public static String searchNoResults = "//div[contains(text(),'There are no results')]";
    public static String  filterTypeBook = "//span[contains(text(),'Book')]";
    public static String buildWorkIdLocator = "//a[@href='/work/%s/overview']";
    public static String buildProductIdLocator="//*[contains(text(),'%s')]/parent::div/preceding-sibling::div/a";
    public static String linkManifestationIdLocator = "//*[contains(text(),'%s')]/parent::div/preceding-sibling::div/a";
}
