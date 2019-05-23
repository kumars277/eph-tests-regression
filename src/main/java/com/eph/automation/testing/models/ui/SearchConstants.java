/**
 * Copyright (C) Estafet Ltd
 */
package com.eph.automation.testing.models.ui;


public final class SearchConstants {

    public static String searchBar = "//*[@id=\"mat-input-0\"]";

    public static String searchOptions = "//*[@id=\"mat-select-0\"]";
    public static String searchOption_Works = "//*[@id=\"mat-option-0\"]";
    public static String searchOption_Manifestations = "//*[@id=\"mat-option-1\"]";
    public static String searchOption_Products = "//*[@id=\"mat-option-2\"]";
    public static String searchOption_People = "//*[@id=\"mat-option-3\"]";

    public static String searchButton = "/html/body/eph-pf-root/div/div[1]/div[2]/div/eph-pf-search/div/form/div/button";

    public static String nextPageButton = "next-button";
    public static String lastPageButton = "last-button";
    public static String previousPageButton = "prev-button";
    public static String firstPageButton = "first-button";
    public static String searchResults = "eph-pf-work-summary";
    public static String searchNoResults = "no-results";


    private SearchConstants() {

    }
}
