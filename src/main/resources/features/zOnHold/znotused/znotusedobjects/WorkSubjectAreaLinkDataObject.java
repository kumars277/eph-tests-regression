package features.zOnHold.znotused.znotusedobjects;

import java.util.Objects;

/**
 * Created by Bistra Drazheva on 18/04/2019
 */
public class WorkSubjectAreaLinkDataObject {
    private String PRODUCT_SUBJECT_AREA_ID;
    private String F_SUBJECT_AREA;
    private String F_PRODUCT_WORK;


    //SA
    private String B_LOADID;
    private String B_CLASSNAME;
    private String WORK_SUBJECT_AREA_LINK_ID;
    private String F_WWORK;

    public String getSTART_DATE() {
        return START_DATE;
    }

    public void setSTART_DATE(String START_DATE) {
        this.START_DATE = START_DATE;
    }

    private String START_DATE;


    public String getUPDATED() {
        return UPDATED;
    }

    public void setUPDATED(String UPDATED) {
        this.UPDATED = UPDATED;
    }

    private String UPDATED;

    public String getEFFTO_DATE() {
        return EFFTO_DATE;
    }

    public void setEFFTO_DATE(String EFFTO_DATE) {
        this.EFFTO_DATE = EFFTO_DATE;
    }

    private String EFFTO_DATE;

    public String external_reference;

    public String getExternal_reference() {
        return external_reference;
    }

    public void setExternal_reference(String external_reference) {
        this.external_reference = external_reference;
    }

    public String getPRODUCT_SUBJECT_AREA_ID() {
        return PRODUCT_SUBJECT_AREA_ID;
    }

    public void setPRODUCT_SUBJECT_AREA_ID(String PRODUCT_SUBJECT_AREA_ID) {
        this.PRODUCT_SUBJECT_AREA_ID = PRODUCT_SUBJECT_AREA_ID;
    }

    public String getF_SUBJECT_AREA() {
        return F_SUBJECT_AREA;
    }

    public void setF_SUBJECT_AREA(String f_SUBJECT_AREA) {
        F_SUBJECT_AREA = f_SUBJECT_AREA;
    }

    public String getF_WWORK() {
        return F_WWORK;
    }

    public void setF_WWORK(String f_WWORK) {
        F_WWORK = f_WWORK;
    }

    public String getF_PRODUCT_WORK() {
        return F_PRODUCT_WORK;
    }

    public void setF_PRODUCT_WORK(String f_PRODUCT_WORK) {
        F_PRODUCT_WORK = f_PRODUCT_WORK;
    }

    public String getB_LOADID() {
        return B_LOADID;
    }

    public void setB_LOADID(String b_LOADID) {
        B_LOADID = b_LOADID;
    }

    public String getB_CLASSNAME() {
        return B_CLASSNAME;
    }

    public void setB_CLASSNAME(String b_CLASSNAME) {
        B_CLASSNAME = b_CLASSNAME;
    }

    public String getWORK_SUBJECT_AREA_LINK_ID() {
        return WORK_SUBJECT_AREA_LINK_ID;
    }

    public void setWORK_SUBJECT_AREA_LINK_ID(String WORK_SUBJECT_AREA_LINK_ID) {
        this.WORK_SUBJECT_AREA_LINK_ID = WORK_SUBJECT_AREA_LINK_ID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkSubjectAreaLinkDataObject that = (WorkSubjectAreaLinkDataObject) o;
        return Objects.equals(PRODUCT_SUBJECT_AREA_ID, that.PRODUCT_SUBJECT_AREA_ID) &&
                Objects.equals(F_SUBJECT_AREA, that.F_SUBJECT_AREA) &&
                Objects.equals(F_PRODUCT_WORK, that.F_PRODUCT_WORK) &&
                Objects.equals(B_LOADID, that.B_LOADID) &&
                Objects.equals(B_CLASSNAME, that.B_CLASSNAME) &&
                Objects.equals(WORK_SUBJECT_AREA_LINK_ID, that.WORK_SUBJECT_AREA_LINK_ID) &&
                Objects.equals(F_SUBJECT_AREA, that.F_SUBJECT_AREA) &&
                Objects.equals(F_WWORK, that.F_WWORK);
    }

    @Override
    public int hashCode() {
        return Objects.hash(PRODUCT_SUBJECT_AREA_ID, F_SUBJECT_AREA, F_PRODUCT_WORK, B_LOADID, B_CLASSNAME, WORK_SUBJECT_AREA_LINK_ID, F_SUBJECT_AREA, F_WWORK);
    }

    @Override
    public String toString() {
        return "WorkSubjectAreaLinkDataObject{" +
                "PRODUCT_SUBJECT_AREA_ID='" + PRODUCT_SUBJECT_AREA_ID + '\'' +
                ", F_SUBJECT_AREA='" + F_SUBJECT_AREA + '\'' +
                ", F_PRODUCT_WORK='" + F_PRODUCT_WORK + '\'' +
                ", B_LOADID='" + B_LOADID + '\'' +
                ", B_CLASSNAME='" + B_CLASSNAME + '\'' +
                ", WORK_SUBJECT_AREA_LINK_ID='" + WORK_SUBJECT_AREA_LINK_ID + '\'' +
                ", F_SUBJECT_AREA='" + F_SUBJECT_AREA + '\'' +
                ", F_WWORK='" + F_WWORK + '\'' +
                '}';
    }
}
