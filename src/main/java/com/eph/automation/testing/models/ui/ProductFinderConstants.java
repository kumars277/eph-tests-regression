package com.eph.automation.testing.models.ui;

import com.eph.automation.testing.models.contexts.DataQualityContext;

public class ProductFinderConstants {

    public static String SCIENCE_ID = "@science.regn.net";
    public static String userDetail="//div[@class='pad-right']";
    public static String loginByEmail = "loginfmt";

    public static String elsevierLogo = "//a/div[@class='elsevier-logo']";
    public static String previousSearchParent = "//div[@class='previous-searches']";
    public static String previousSearchChild = previousSearchParent+"/ul/li";
    public static String latestWorkParent = "//div[@class='latest-works']";
    public static String latestWorkChild = latestWorkParent+"//div[contains(@class,'search-result')]";
    public static String latestWorkViewMore = latestWorkParent+"/div[contains(@class,'view-more')]/span";
    public static String helpSupportParent = "//div[@class='help-support']";
    public static String helpSupportChild = helpSupportParent+"/ul/li/a";
    public static String searchBar = " //input[@id='mat-input-0']";
    public static String searchButton = "//i[@class='gm-ico-search']";
    public static String tab_Works="//div[@class='mat-tab-links']//a[contains(text(),'Work')]";
    public static String tab_product_andPackages="//div[@class='mat-tab-links']//a[contains(text(),'Products')]";
    public static String tab_manifestation="//div[@class='mat-tab-links']//a[contains(text(),'Manifestations')]";
    public static String tab_Overview = "//div[@class='mat-tab-links']//a[contains(text(),'Overview')]";
    public static String tab_Packages = "//div[@class='mat-tab-links']//a[contains(text(),'Packages')]";
    public static String tab_RelatedManifestations = "//div[@class='mat-tab-links']//a[contains(text(),'Related Manifestations')]";

    public static String resultPerPage_100="//span[@class='ng-star-inserted']/a[contains(text(),'100')]|//div[@class='pages']//a[contains(text(),'100')]";
    public static String resultPerPage_50="//span[@class='ng-star-inserted']/a[contains(text(),'50')]|//div[@class='pages']//a[contains(text(),'50')]";
    public static String SearchResultPageCount="//div[@class='page-label']";
    public static String nextPageButton = "//div[@class='next-button']";
    public static String nextButton = "idSIButton9";
    public static String itemDetail="(//div[contains(@class,'item-detail')])";
    public static String itemDetailInner="//div[@class='ng-star-inserted']";
    public static String itemidentifier="//div[contains(@class,'identifiers')]/child::div";
    public static String searchNoResults = "//div[contains(text(),'There are no results')]";
    public static String searchSuggesion="//div[contains(@class,'search-suggestion')]";
    public static String  filterTypeBook = "//span[contains(text(),'Book')]";
//    public static String buildWorkIdLocator = "//a[contains(@href,'/work/%s/overview')]";
    public static String buildIdLocator = "//a[contains(@href,'/%s/overview')]";
    public static String buildProductIdLocator="//*[contains(text(),'%s')]/parent::div/preceding-sibling::div/a";
    public static String linkManifestationIdLocator = "//*[contains(text(),'%s')]/parent::div/preceding-sibling::div/a";

    public static String FilterLocator = "//div[@class='filter-panel']/form/child::section";

    public static String coreTab = "//*[@id='mat-tab-label-0-0']/div";
    public static String peopleTab = "//*[@id='mat-tab-label-0-1']/div";
    public static String financialTab = "//*[@id='mat-tab-label-0-2']/div";
    public static String editorialTab = "//*[@id='mat-tab-label-0-3']/div";
    //public static String linkTab="//*[@id='mat-tab-label-0-4']/div";
    //added by Nishant @ 23Oct 2020
    public static String linkTab="//*[@id='mat-tab-label-0-4']/div|//*[boolean(number(substring-before(substring-after(@id, \"mat-tab-label-\"), \"-4\")))]";

    //String section = "//div[@class='section']"; //parent of - Editorial Information
    //public static String section = "//div[contains(@class,'section')]"; //parent of - subject area and information,//parent of - Financial Information

    public static String section = "//div[@class='section']"; //parent Information
    public static String sectionDetailDiv = "/div[@class='section-detail']";
    public static String DetailInformation1 = section + sectionDetailDiv+"[1]";
    public static String DetailInformation2 = section + sectionDetailDiv+"[2]";

    public static String sectionDetail = section + sectionDetailDiv; //core information

    public static String section_identifier = "//div[@class='section']";
    public static String subAreaRow = section_identifier+"//div[@class='section-detail subject-areas']";
    public static String identifierRow = section_identifier+"/eph-pf-identifier-list"+sectionDetailDiv;
    public static String specialtiesRow = section_identifier+"/eph-pf-specialties"+sectionDetailDiv;


    public static String sectionDetailJF = section+"[1]"+sectionDetailDiv;
    public static String subAreaRowJF = section+"[2]"+"//div[@class='section-detail subject-areas']";
    public static String identifierRowJF = section_identifier+"/eph-pf-identifier-list"+sectionDetailDiv;
    public static String specialtiesRowJF = section+"[2]"+"/eph-pf-specialties"+"//ul[@id='specialties']";

    public static String DetailIdentifiers = section_identifier + sectionDetailDiv;
    //table[@class='mat-table']/parent::div[@class='section-detail']/following-sibling::h2

    public static String searchDropdownPerson="//option[@value='personName']|//option[@value='personFullNameCurrent']";
    public static String searchDropdownPmg="//option[@value='pmgCode']";
    public static String searchDropdownPmc="//option[@value='pmcCode']";
    public static String zeroResultFound="//div[@class='container search-results no-results']";
    public static String productFoundOf="//div[@class='pager']//div[@class='container']/div[1]";



    public static String journalSearchbar = "//div[@class='search-input']//child::input";
    public static String journalsearchButton = "//button[@class='pf-icon-button s46 primary search-button mat-ripple']";

    public static String manifestation_page_click ="(//div[@class='ng-star-inserted'])[3]";






}
