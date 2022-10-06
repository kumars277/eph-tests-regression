package com.eph.automation.testing.models.dao;

public class PkgItemsAccessObject {

    private String product_id;
    private String epr_id;
    private String issn;
    private String journal_number;
    private String ownership_type;
    private String f_pmg;
    private String pmg_code;
    private String publisher;
    private String publishing_director;
    private String title;
    private String legal_ownership;
    private String legal_ownership_type;
    private String work_title;

    public String getproduct_id() {
        return product_id;
    }
    public void setproduct_id(String product_id) {
        this.product_id = product_id;
    }

    public String getwork_title() {
        return work_title;
    }
    public void setwork_title(String work_title) {
        this.work_title = work_title;
    }

    public String getlegal_ownership() {
        return legal_ownership;
    }
    public void setlegal_ownership(String legal_ownership) {
        this.legal_ownership = legal_ownership;
    }

    public String getlegal_ownership_type() {
        return legal_ownership_type;
    }
    public void setlegal_ownership_type(String legal_ownership_type) {
        this.legal_ownership_type = legal_ownership_type;
    }

    public String getepr_id() {
        return epr_id;
    }
    public void setepr_id(String epr_id) {
        this.epr_id = epr_id;
    }

    public String getissn() {
        return issn;
    }
    public void setissn(String issn) {
        this.issn = issn;
    }

    public String gettitle() {
        return title;
    }
    public void settitle(String title) {
        this.title = title;
    }

    public String getjournal_number() {
        return journal_number;
    }
    public void setjournal_number(String journal_number) {
        this.journal_number = journal_number;
    }

    public String getownership_type() {
        return ownership_type;
    }
    public void setownership_type(String ownership_type) {
        this.ownership_type = ownership_type;
    }

    public String getf_pmg() {
        return f_pmg;
    }
    public void setf_pmg(String f_pmg) {
        this.f_pmg = f_pmg;
    }

    public String getpmg_code() {
        return pmg_code;
    }
    public void setpmg_code(String pmg_code) {
        this.pmg_code = pmg_code;
    }

    public String getpublisher() {
        return publisher;
    }
    public void setpublisher(String productType) {
        this.publisher = publisher;
    }

    public String getpublishing_director() {
        return publishing_director;
    }
    public void setpublishing_director(String publishing_director) {
        this.publishing_director = publishing_director;
    }



}
