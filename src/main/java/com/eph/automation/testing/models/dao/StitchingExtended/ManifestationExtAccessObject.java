package com.eph.automation.testing.models.dao.StitchingExtended;

public class ManifestationExtAccessObject {

    private String epr_id;
    private String manifestation_type;
    private String last_updated_date;
    private boolean uk_textbook_ind;
    private boolean us_textbook_ind;
    private String manifestation_trim_text;
    private String commodity_code;
    private String discount_code_emea;
    private String discount_code_us;
    private String manifestation_weight;
    private String journal_issue_trim_size;
    private String war_reference;
    private String type;

    private String count_type_code;
    private String count_type_name;
    private String count;

    private String restriction_name;
    private String restriction_code;

    public String getrestriction_name() {
        return restriction_name;
    }
    public void setrestriction_name(String restriction_name) {
        this.restriction_name = restriction_name;
    }

    public String getrestriction_code() {
        return restriction_code;
    }
    public void setrestriction_code(String restriction_code) {
        this.restriction_code = restriction_code;
    }

    public String getcount_type_code() {
        return count_type_code;
    }
    public void setcount_type_code(String count_type_code) {
        this.count_type_code = count_type_code;
    }

    public String getcount_type_name() {
        return count_type_name;
    }
    public void setcount_type_name(String count_type_name) {
        this.count_type_name = count_type_name;
    }

    public String getcount() {
        return count;
    }
    public void setcount(String count) {
        this.count = count;
    }

    public String getmanifestation_type() {
        return manifestation_type;
    }
    public void setmanifestation_type(String manifestation_type) {
        this.manifestation_type = manifestation_type;
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

    public boolean getuk_textbook_ind() {
        return uk_textbook_ind;
    }
    public void setuk_textbook_ind(boolean uk_textbook_ind) {
        this.uk_textbook_ind = uk_textbook_ind;
    }

    public String getepr_id() { return epr_id; }
    public void setepr_id(String epr_id) {
        this.epr_id = epr_id;
    }

    public String getmanifestation_trim_text() { return manifestation_trim_text; }
    public void setmanifestation_trim_text(String manifestation_trim_text) {
        this.manifestation_trim_text = manifestation_trim_text;
    }

    public String getcommodity_code() {
        return commodity_code;
    }
    public void setcommodity_code(String commodity_code) {
        this.commodity_code = commodity_code;
    }

    public boolean getus_textbook_ind() {
        return us_textbook_ind;
    }
    public void setus_textbook_ind(boolean us_textbook_ind) {
        this.us_textbook_ind = us_textbook_ind;
    }

    public String getdiscount_code_emea() {
        return discount_code_emea;
    }
    public void setdiscount_code_emea(String discount_code_emea) {
        this.discount_code_emea = discount_code_emea;
    }

    public String getdiscount_code_us() {
        return discount_code_us;
    }
    public void setdiscount_code_us(String discount_code_us) {
        this.discount_code_us = discount_code_us;
    }

    public String getmanifestation_weight() {
        return manifestation_weight;
    }
    public void setmanifestation_weight(String manifestation_weight) {
        this.manifestation_weight = manifestation_weight;
    }

    public String getjournal_issue_trim_size() {
        return journal_issue_trim_size;
    }
    public void setjournal_issue_trim_size(String journal_issue_trim_size) {
        this.journal_issue_trim_size = journal_issue_trim_size;
    }

    public String getwar_reference() {
        return war_reference;
    }
    public void setwar_reference(String war_reference) {
        this.war_reference = war_reference;
    }

}

