package com.eph.automation.testing.models.ui;

public class ResearchPackagesConstants {

    public static String LoginByEmail = "loginfmt";
    public static  String NextButton = "idSIButton9";
    public static String USER_LOGIN_ICON = "//*[starts-with(@class,'gm-ico-person')]";
    public static String IMPERSONATE_BUTTON = "//button[contains(text(),'Impersonate Other User')]";
    public static String ENTER_USER_DROPDOWN = "//div[@class='mat-select-value']";
    public static String CHOOSE_USER_DROPDOWN = "//span[contains(text(),'Dinesh')]";
    public static String CHOOSE_PRODUCT_OWNER = "//div[contains(text(),'Product Owner')]";
    public static String SUBMIT_USER = "//button[contains(text(),'Select')]";
    public static String SELECT_DJC = "//div[contains(text(),'DJC')]";
    public static String SELCT_SJC  = "//div[contains(text(),'SJC')]";
    public static String SELECT_CC  = "//div[contains(text(),'CC')]";
    public static String SELECT_CC_BASIC = "//div[contains(text(),' Clinics Collection (ScienceDirect) - Basic ')]";
    public static String SELCT_MCC  = "//div[contains(text(),'MCC')]";
    public static String ADD_JOURNAL = "//button[contains(text(),'Add journal')]";
    public static String SEARCH_VALUE = "//input[@placeholder='Item search']";
    public static String SEARCH_SUBMIT = "//button[@type='submit']";
    public static String INCLUDE_JOURNAL = "//div[@class='clickable' and @title='Include']";
    public static String INCLUDE_JOURNAL_POPUP = "//div[@class='display clickable' and @title='Include']";
    public static String CHOOSE_REASON_DROPDOWN = "//span[contains(text(),'Reason for status')]";
    public static String CHOOSE_REASON_INCLUDE    = "//span[contains(text(),'New Journal')]";
    public static String CHOOSE_REASON_EXCLUDE    = "//span[contains(text(),'Transfer Out')]";
    public static String ADD = "//button[contains(text(),'Add') and (@class='pf-icon-button primary')]";
    public static String EXCLUDE_ADD = "//button[contains(text(),'Add') and (@class='pf-icon-button primary ng-star-inserted')]";
    public static String SAVE_COLLECTIONS = "//button[contains(text(),'Save')]";
    public static String NO_COLLECTIONS = "//div[contains(text(), 'There is currently no active collection')]";
    public static String CREATE_PROSPECTIVE_LIST = "//span[contains(text(),'Create Prospective List')]";
    public static String SEARCH_FILTER  = "mat-input-0";
    public static String CHECK_IS_EXCLUDE = "//*[@class='is-exclude']";
    public static String EXCLUDE_JOURNAL = "//div[@title='Exclude' and @class='clickable']";
    public static String CHECK_IS_PENDING = "//*[@class='is-pending']";
    public static String PENDING_JOURNAL_POPUP = "//div[@class='display clickable' and @title='Pending']";
    public static String PENDING_JOURNAL =      "//div[@class='clickable' and @title='Pending']";
    public static String CHOOSE_REASON_PENDING = "//span[contains(text(),'Awaiting final decision')]";
    public static String ADD_REMARKS = "//textarea[@formcontrolname='remarks']";
    public static String SCIENCE_ID = "SURESHKUMARD@science.regn.net";
    public static String resultTable  = "tbody";
//mat-checkbox[@id='mat-checkbox-212']//div[@class='mat-checkbox-inner-container mat-checkbox-inner-container-no-side-margin']

public static String pendingFilterCheckbox = "//label[@for='mat-checkbox-8-input']";
public static String excludeFilterCheckbox = "//label[@for='mat-checkbox-10-input']";
public static String unsavedFilterCheckbox = "/html[1]/body[1]/eph-rp-root[1]/div[1]/div[1]/mat-sidenav-container[1]/mat-sidenav-content[1]/div[1]/div[1]/div[2]/div[1]/eph-rp-summary[1]/div[1]/div[1]/eph-rp-prospective-list[1]/div[1]/div[1]/div[2]/eph-rp-list-filter[1]/div[1]/div[3]/div[1]/div[2]/div[5]/div[1]/div[1]/mat-checkbox[1]/label[1]/div[1]";
    public static String includeFilterCheckbox = "//label[@for='mat-checkbox-9-input']";
public static String PubDirector_Filter_Rapes_Relaxed = "//label[@for='mat-checkbox-16-input']";
public static String PubDirector_Filter_Rapes_Relaxed_count = "//div[contains(text(),'Rapes Relaxed')]/following-sibling::div";
public static String Ownership_Filter_ELSOWN = "//label[@for='mat-checkbox-24-input']";
public static String Ownership_Filter_ELSOWN_Count="//div[contains(text(),'ELSOWN')]/following-sibling::div";
    public static String PMG_Filter_MCC = "//label[@for='mat-checkbox-20-input']";
    public static String PMG_FILTER_MCC_COUNT="//div[contains(text(),'600')]/following-sibling::div";
    public static String PENDING_FILTER_COUNT="//div[contains(text(),'Pending')]/following-sibling::div";
    public static String EXCLUDE_FILTER_COUNT="//div[contains(text(),'Excluded')]/following-sibling::div";
    public static String INCLUDE_FILTER_COUNT="//div[contains(text(),'Included')]/following-sibling::div";
    public static String UNSAVE_FILTER_COUNT="//div[contains(text(),'Unsaved change')]/following-sibling::div";
    public static String UNSAVED_JOURNAL_TEXT = "//span[@class='ng-star-inserted' and contains(text(),'unsaved')]";
    public static String UNSAVED_ROW = "//tr[contains(@class,'is-dirty')]";
    public static String IS_PENDING = "//div[@class='is-pending']";
    public static String IS_EXCLUDED = "//div[@class='is-exclude']";
    public static String IS_INCLUDED = "//div[@class='is-include']";
    public static String ROW_RESULT = "//div[@class='mat-row ng-star-inserted']";
    public static String ISSN_COLUMN = "//td[contains(@class,'cdk-column-issn')]";
    public static String PUBLISHER_COLUMN =    "//td[contains(@class,'cdk-column-publisher')]";
    public static String OWNER_COLUMN  = "//td[contains(@class,'cdk-column-ownershipType')]";
    public static String PMG_COLUMN = "//td[contains(@class,'cdk-column-pmgCode')]";
    public static String PUBDIRECTOR_COLUMN = "//td[contains(@class,'cdk-column-publishingDirector')]";
    public static String TITLE_COLUMN = "//td[contains(@class,'cdk-column-title')]";
    public static String JOURNALNO_COLUMN = "//td[contains(@class,'cdk-column-journalNumber')]";

}
