package com.eph.automation.testing.models.dao.SDBooksDataLake;

public class SDBooksDLAccessObject {

    private String ISBN;
    private String BOOK_TITLE;
    private String URL;
    private String URL_CODE;
    private String URL_NAME;
    private String URL_TITLE;
    private String EPRID;
    private String WORK_TYPE;
    private String DELTA_MODE;


    public String getISBN() {
        return ISBN;
    }
    public void setISBN(String ISBN) {
        this.ISBN = ISBN;
    }

    public String getBOOK_TITLE() {
        return BOOK_TITLE;
    }
    public void setBOOK_TITLE(String BOOK_TITLE) {
        this.BOOK_TITLE = BOOK_TITLE;
    }

    public String getURL() {
        return URL;
    }
    public void setURL(String URL) {
        this.URL = URL;
    }

    public String getURL_CODE() {
        return URL_CODE;
    }
    public void setURL_CODE(String URL_CODE) {
        this.URL_CODE = URL_CODE;
    }

    public String getURL_NAME() {
        return URL_NAME;
    }
    public void setURL_NAME(String URL_NAME) {
        this.URL_NAME = URL_NAME;
    }

    public String getURL_TITLE() {
        return URL_TITLE;
    }
    public void setURL_TITLE(String URL_TITLE) {
        this.URL_TITLE = URL_TITLE;
    }

    public String getEPRID() {
        return EPRID;
    }
    public void setEPRID(String EPRID) {
        this.EPRID = EPRID;
    }

    public String getWORK_TYPE() {
        return WORK_TYPE;
    }
    public void setWORK_TYPE(String WORK_TYPE) {
        this.WORK_TYPE = WORK_TYPE;
    }

    public String getDELTA_MODE() {
        return DELTA_MODE;
    }
    public void setDELTA_MODE(String DELTA_MODE) {
        this.DELTA_MODE = DELTA_MODE;
    }



}

