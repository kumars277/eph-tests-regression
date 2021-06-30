package com.eph.automation.testing.models.dao.JMDataLake;

public class JM_ETLExtendedDLAccessObject {

    private String epr_id;
    private String product_type;
    private String last_updated_date;
    private String application_name;
    private String availability_start_date;
    private String availability_status;
    private String delete_flag;
    private String application_code;
    private String issn;
    private int eprIdCount;

    private String source;
    private String getlast_updated_date;
    private String getapplication_name;
    private String getdelta_answer_code_uk;
    private String getdelta_answer_code_us;
    private String getpublication_status_anz;
    private String getavailability_format;
    
    
    public String getsource() { return source; }
    public void setsource(String source) {
        this.source = source;
    }

    public String getgetlast_updated_date() { return getlast_updated_date; }
    public void setgetlast_updated_date(String getlast_updated_date) {
        this.getlast_updated_date = getlast_updated_date;
    }

    public String getgetapplication_name() { return getapplication_name; }
    public void setgetapplication_name(String getapplication_name) {
        this.getapplication_name = getapplication_name;
    }

    public String getdelta_answer_code_uk() { return getdelta_answer_code_uk; }
    public void setgetdelta_answer_code_uk(String getdelta_answer_code_uk) {
        this.getdelta_answer_code_uk = getdelta_answer_code_uk;
    }

    public String getdelta_answer_code_us() { return getdelta_answer_code_us; }
    public void setgetdelta_answer_code_us(String getdelta_answer_code_us) {
        this.getdelta_answer_code_us = getdelta_answer_code_us;
    }

    public String getpublication_status_anz() { return getpublication_status_anz; }
    public void setgetpublication_status_anz(String getpublication_status_anz) {
        this.getpublication_status_anz = getpublication_status_anz;
    }

    public String getavailability_format() { return getavailability_format; }
    public void setgetavailability_format(String getavailability_format) {
        this.getavailability_format = getavailability_format;
    }
    
    public int geteprIdCount() { return eprIdCount; }
    public void seteprIdCount(int eprIdCount) {
        this.eprIdCount = eprIdCount;
    }


    public String getepr_id() { return epr_id; }
    public void setepr_id(String epr_id) {
        this.epr_id = epr_id;
    }

    public String getissn() { return issn; }
    public void setissn(String issn) {
        this.issn = issn;
    }


    public String getapplication_code() { return application_code; }
    public void setapplication_code(String application_code) {
        this.application_code = application_code;
    }

    public String getproduct_type() { return product_type; }
    public void setproduct_type(String product_type) {
        this.product_type = product_type;
    }

    public String getlast_updated_date() { return last_updated_date; }
    public void setlast_updated_date(String last_updated_date) {
        this.last_updated_date = last_updated_date;
    }

    public String getapplication_name() { return application_name; }
    public void setapplication_name(String application_name) {
        this.application_name = application_name;
    }

    public String getavailability_start_date() { return availability_start_date; }
    public void setavailability_start_date(String availability_start_date) {
        this.availability_start_date = availability_start_date;
    }

    public String getavailability_status() { return availability_status; }
    public void setavailability_status(String availability_status) {
        this.availability_status = availability_status;
    }

    public String getdelete_flag() { return delete_flag; }
    public void setdelete_flag(String delete_flag) {
        this.delete_flag = delete_flag;
    }


}

