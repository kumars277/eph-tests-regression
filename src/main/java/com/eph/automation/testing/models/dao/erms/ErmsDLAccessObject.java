package com.eph.automation.testing.models.dao.erms;

public class ErmsDLAccessObject {

    private String erms_id;
    private String epr_id;
    private String u_key;
    private String work_source_ref;
    private String eph_work_id;
    private String erms_person_ref;
    private String person_source_ref;
    private String f_role;
    private String email;
    private String name;
    private String staff_user;
    private String effective_start_date;
    private String effective_end_date;
    private String modified_date;
    private String is_deleted;

    public String getname() {
        return name;
    }
    public void setname(String name) {
        this.name = name;
    }

    public String getis_deleted() {return is_deleted;}
    public void setis_deleted(String is_deleted) {
        this.is_deleted = is_deleted;
    }

    public String getstaff_user() {
        return staff_user;
    }
    public void setstaff_user(String staff_user) {
        this.staff_user = staff_user;
    }

    public String geteffective_start_date() { return effective_start_date;    }
    public void seteffective_start_date(String effective_start_date) {
        this.effective_start_date = effective_start_date;
    }

    public String geteffective_end_date() { return effective_end_date;    }
    public void seteffective_end_date(String effective_end_date) {
        this.effective_end_date = effective_end_date;
    }

    public String getmodified_date() { return modified_date;    }
    public void setmodified_date(String modified_date) {
        this.modified_date = modified_date;
    }

    public String geterms_id() {
        return erms_id;
    }
    public void seterms_id(String erms_id) {
        this.erms_id = erms_id;
    }

    public String getepr_id() {
        return epr_id;
    }
    public void setepr_id(String epr_id) {
        this.epr_id = epr_id;
    }

    public String getu_key() {
        return u_key;
    }
    public void setu_key(String u_key) {
        this.u_key = u_key;
    }

    public String getwork_source_ref() {
        return work_source_ref;
    }
    public void setwork_source_ref(String work_source_ref) {
        this.work_source_ref = work_source_ref;
    }

    public String geteph_work_id() {
        return eph_work_id;
    }
    public void seteph_work_id(String eph_work_id) {
        this.eph_work_id = eph_work_id;
    }

    public String geterms_person_ref() {
        return erms_person_ref;
    }
    public void seterms_person_ref(String erms_person_ref) {
        this.erms_person_ref = erms_person_ref;
    }

    public String getperson_source_ref() {
        return person_source_ref;
    }
    public void setperson_source_ref(String person_source_ref) {
        this.person_source_ref = person_source_ref;
    }

    public String getf_role() {
        return f_role;
    }
    public void setf_role(String f_role) {
        this.f_role = f_role;
    }

    public String getemail() {
        return email;
    }
    public void setemail(String email) {
        this.email = email;
    }



}

