package com.eph.automation.testing.models.dao;

public class FinancialAttribsDataObject {
    public String random_value;
    public String opco;
    public String resp_centre;


    public String getPMX_SOURCE_REFERENCE() {
        return PMX_SOURCE_REFERENCE;
    }

    public void setPMX_SOURCE_REFERENCE(String PMX_SOURCE_REFERENCE) {
        this.PMX_SOURCE_REFERENCE = PMX_SOURCE_REFERENCE;
    }

    public String getWorkID() {
        return workID;
    }

    public void setWorkID(String workID) {
        this.workID = workID;
    }

    public String workID;
    public String PMX_SOURCE_REFERENCE;

    public String getRandom_value() {
        return random_value;
    }

    public void setRandom_value(String random_value) {
        this.random_value = random_value;
    }

    public String getOpco() {
        return opco;
    }

    public void setOpco(String opco) {
        this.opco = opco;
    }

    public String getResp_centre() {
        return resp_centre;
    }

    public void setResp_centre(String resp_centre) {
        this.resp_centre = resp_centre;
    }

    public String getB_CLASSNAME() {
        return B_CLASSNAME;
    }

    public void setB_CLASSNAME(String b_CLASSNAME) {
        B_CLASSNAME = b_CLASSNAME;
    }


    public String getGl_company() {
        return gl_company;
    }

    public void setGl_company(String gl_company) {
        this.gl_company = gl_company;
    }

    public String getCost_resp_centre() {
        return cost_resp_centre;
    }

    public void setCost_resp_centre(String cost_resp_centre) {
        this.cost_resp_centre = cost_resp_centre;
    }

    public String getRevenue_resp_centre() {
        return revenue_resp_centre;
    }

    public void setRevenue_resp_centre(String revenue_resp_centre) {
        this.revenue_resp_centre = revenue_resp_centre;
    }

    public String getWork_id() {
        return work_id;
    }

    public void setWork_id(String work_id) {
        this.work_id = work_id;
    }

    public String B_CLASSNAME;


    public String getFin_attribs_id() {
        return fin_attribs_id;
    }

    public void setFin_attribs_id(String fin_attribs_id) {
        this.fin_attribs_id = fin_attribs_id;
    }

    public String fin_attribs_id;
    public String gl_company;
    public String cost_resp_centre;
    public String revenue_resp_centre;
    public String work_id;

}