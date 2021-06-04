package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 16 Feb 2021, EPHD-2747
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.SubjectAreaDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SubjectAreasApiObject {

    public static List<SubjectAreaDataObject> subjectAreaDataObjectsFromGD_child;
    public static List<SubjectAreaDataObject> subjectAreaDataObjectsFromGD_parent;
    public SubjectAreasApiObject() {}

    private SubjectAreaApiObject subjectArea;
    public SubjectAreaApiObject getSubjectArea() {return subjectArea;}
    public void setSubjectArea(SubjectAreaApiObject subjectArea) {this.subjectArea = subjectArea;}

    private String effectiveStartDate;
    public String getEffectiveStartDate() {return effectiveStartDate;}
    public void setEffectiveStartDate(String effectiveStartDate) {this.effectiveStartDate = effectiveStartDate;}

    private String effectiveEndDate;
    public String getEffectiveEndDate() {return effectiveEndDate;}
    public void setEffectiveEndDate(String effectiveEndDate) {this.effectiveEndDate = effectiveEndDate;}

    private Boolean primaryInd;
    public Boolean getPrimaryInd() {return primaryInd;}
    public void setPrimaryInd(Boolean primaryInd) {this.primaryInd = primaryInd;}



    public void compareWithDB(String workID){
        System.out.println("subjectArea Code - "+this.subjectArea.getCode());
        getSubjectAreaDataEPHGDbyCode(this.subjectArea.getCode());
        Assert.assertEquals(this.subjectArea.getName(), subjectAreaDataObjectsFromGD_child.get(0).getSUBJECT_AREA_NAME());
        printLog("subject Area Name");

        Assert.assertEquals(this.subjectArea.getType().get("code"), subjectAreaDataObjectsFromGD_child.get(0).getSUBJECT_AREA_TYPE());
        printLog("subject Area Type code");

        getParentSubjectAreaDataEPHGDbyCode(this.subjectArea.getParentSubjectArea().get("code").toString());

        Assert.assertEquals(this.subjectArea.getParentSubjectArea().get("name"), subjectAreaDataObjectsFromGD_parent.get(0).getSUBJECT_AREA_NAME());
        printLog("parent subject Area Name");
    }


    private void getSubjectAreaDataEPHGDbyCode(String code) {
       // Log.info("Get the subject area data from EPH GD  ..");
        String sql = String.format(APIDataSQL.EXTRACT_DATA_SUBJECT_AREA_GD_BY_CODE, code);
     //   Log.info(sql);

        subjectAreaDataObjectsFromGD_child = DBManager
                .getDBResultAsBeanList(sql, SubjectAreaDataObject.class, Constants.EPH_URL);
    }

    private void getParentSubjectAreaDataEPHGDbyCode(String code) {
       // Log.info("Get the subject area data from EPH GD  ..");
        String sql = String.format(APIDataSQL.EXTRACT_DATA_SUBJECT_AREA_GD_BY_CODE, code);
     //   Log.info(sql);

        subjectAreaDataObjectsFromGD_parent = DBManager
                .getDBResultAsBeanList(sql, SubjectAreaDataObject.class, Constants.EPH_URL);
    }

    private void printLog(String verified) {Log.info("verified..." + verified);}

}