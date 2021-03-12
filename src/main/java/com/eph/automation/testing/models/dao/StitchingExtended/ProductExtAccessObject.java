package com.eph.automation.testing.models.dao.StitchingExtended;

public class ProductExtAccessObject {

    private String epr_id;
    private String product_type;
    private String last_updated_date;
    private String application_name;
    private String delta_answer_code_uk;
    private String delta_answer_code_us;
    private String publication_status_anz;
    private String availability_format;
    private String availability_start_date;
    private String availability_status;
    private String type;


    public String getproduct_type() {
        return product_type;
    }
    public void setproduct_type(String product_type) {
        this.product_type = product_type;
    }

    public String getapplication_name() {
        return application_name;
    }
    public void setapplication_name(String application_name) {
        this.application_name = application_name;
    }

    public String getdelta_answer_code_uk() {
        return delta_answer_code_uk;
    }
    public void setdelta_answer_code_uk(String delta_answer_code_uk) {
        this.delta_answer_code_uk = delta_answer_code_uk;
    }

    public String getdelta_answer_code_us() {
        return delta_answer_code_us;
    }
    public void setdelta_answer_code_us(String delta_answer_code_us) {
        this.delta_answer_code_us = delta_answer_code_us;
    }

    public String getpublication_status_anz() {
        return publication_status_anz;
    }
    public void setpublication_status_anz(String publication_status_anz) {
        this.publication_status_anz = publication_status_anz;
    }

    public String getavailability_format() {
        return availability_format;
    }
    public void setavailability_format(String availability_format) {
        this.availability_format = availability_format;
    }

    public String gettype() {
        return type;
    }
    public void settype(String type) {
        this.type = type;
    }

    public String getlast_updated_date() {
        return last_updated_date;
    }
    public void setlast_updated_date(String last_updated_date) {
        this.last_updated_date = last_updated_date;
    }

    public String getavailability_start_date() {
        return availability_start_date;
    }
    public void setavailability_start_date(String availability_start_date) {
        this.availability_start_date = availability_start_date;
    }

    public String getepr_id() { return epr_id; }
    public void setepr_id(String epr_id) {
        this.epr_id = epr_id;
    }

    public String getavailability_status() { return availability_status; }
    public void setavailability_status(String availability_status) {
        this.availability_status = availability_status;
    }

}

