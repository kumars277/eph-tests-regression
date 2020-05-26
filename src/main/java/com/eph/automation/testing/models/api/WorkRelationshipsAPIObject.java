package com.eph.automation.testing.models.api;

/*
* Created by Nishant @ 5 May 2020
* */

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.eph.automation.testing.services.db.sql.WorkRelationshipDataObject;
import com.eph.automation.testing.services.db.sql.WorkRelationshipSQL;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import org.junit.Assert;

import java.util.Comparator;
import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkRelationshipsAPIObject {

    public WorkRelationshipsAPIObject(){}
    private DataQualityContext dataQualityContext;

    private workParent[] workParent;
    public WorkRelationshipsAPIObject.workParent[] getWorkParent() {return workParent;}
    public void setWorkParent(WorkRelationshipsAPIObject.workParent[] workParent) {this.workParent = workParent;}

    private workChild[] workChild;
    public WorkRelationshipsAPIObject.workChild[] getWorkChild() {return workChild;}
    public void setWorkChild(WorkRelationshipsAPIObject.workChild[] workChild) {this.workChild = workChild;}

    public void compareWithDB(String WorkId) {
        Log.info("verifying workRelationship..." + WorkId);
        if (workParent != null) {
            getWorkRelationshipParentRecordsEPHGD(WorkId);
            for (workParent parent : workParent) {
                printLog("workParent "+parent.id);
                //Log.info("-Relationship type code\n-Relationship effectiveStartDate");
                Assert.assertEquals(parent.type.get("code"), dataQualityContext.workRelationshipParentDataObjectsFromEPGD.get(0).getF_RELATIONSHIP_TYPE());
                printLog("relationshipType");

                Assert.assertEquals(parent.effectiveStartDate, dataQualityContext.workRelationshipParentDataObjectsFromEPGD.get(0).getEFFECTIVE_START_DATE());
                printLog("effectiveStartDate");

                getWorksDataFromEPHGD(parent.id);
                //Log.info("comparing\n-title\n-work Type code\n-work status code");
                Assert.assertEquals(parent.workSummary.title, dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
                printLog("work Title");

                Assert.assertEquals(parent.workSummary.type.get("code"), dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
                printLog("work Type");

                Assert.assertEquals(parent.workSummary.status.get("code"), dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
                printLog("work Status");
            }
        }
        if (workChild != null) {
            getWorkRelationshipChildRecordsEPHGD(WorkId);
            for (workChild child : workChild) {

                printLog("workChild "+child.id);
               // Log.info("-Relationship type code\n-Relationship effectiveStartDate");
                Assert.assertEquals(child.type.get("code"), dataQualityContext.workRelationshipChildDataObjectsFromEPGD.get(0).getF_RELATIONSHIP_TYPE());
                printLog("relationshipType");

                Assert.assertEquals(child.effectiveStartDate, dataQualityContext.workRelationshipChildDataObjectsFromEPGD.get(0).getEFFECTIVE_START_DATE());
                printLog("effectiveStartDate");

                getWorksDataFromEPHGD(child.id);
                Log.info("comparing \n-title\n-work Type code\n-work status code");
                Assert.assertEquals(child.workSummary.title, dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TITLE());
                printLog("work Title");

                Assert.assertEquals(child.workSummary.type.get("code"), dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_TYPE());
                printLog("work Type");

                Assert.assertEquals(child.workSummary.status.get("code"), dataQualityContext.workDataObjectsFromEPHGD.get(0).getWORK_STATUS());
                printLog("work Status");
            }

        }
    }

    public void getWorkRelationshipParentRecordsEPHGD(String workId){
        Log.info("getParent record of..."+workId);
        String sql=String.format(WorkRelationshipSQL.getWorkParent,workId);
        dataQualityContext.workRelationshipParentDataObjectsFromEPGD = DBManager.getDBResultAsBeanList(sql, WorkRelationshipDataObject.class, Constants.EPH_URL);
    }

    public void getWorkRelationshipChildRecordsEPHGD(String workId){
        Log.info("getChild record of..."+workId);
        String sql=String.format(WorkRelationshipSQL.getWorkChild,workId);
        dataQualityContext.workRelationshipChildDataObjectsFromEPGD = DBManager.getDBResultAsBeanList(sql, WorkRelationshipDataObject.class, Constants.EPH_URL);
    }

    public void getWorksDataFromEPHGD(String workId) {
        Log.info("getWork record ..."+workId);
        String sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, workId);
        dataQualityContext.workDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        dataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getWORK_ID));
        Assert.assertFalse("Verify that list with work objects from DB is not empty", dataQualityContext.workDataObjectsFromEPHGD.isEmpty());
    }

    public static class workParent{
        private HashMap<String, Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}

        private String id;
        public String getId() {return id;}
        public void setId(String id) {this.id = id;}

        private workSummary workSummary;
        public WorkRelationshipsAPIObject.workSummary getWorkSummary() {return workSummary;}
        public void setWorkSummary(WorkRelationshipsAPIObject.workSummary workSummary) {this.workSummary = workSummary;}

        private String effectiveStartDate;
        public String getEffectiveStartDate() {return effectiveStartDate;}
        public void setEffectiveStartDate(String effectiveStartDate) {this.effectiveStartDate = effectiveStartDate;}

        private String  effectiveEndDate;
        public String getEffectiveEndDate() {return effectiveEndDate;}
        public void setEffectiveEndDate(String effectiveEndDate) {this.effectiveEndDate = effectiveEndDate;}

    }

    public static class workChild{
        private HashMap<String, Object> type;
        public HashMap<String, Object> getType(){return type;}
        public void setType(HashMap<String,Object> type){this.type=type;}

        private String id;
        public String getId(){return id;}
        public void setId(String id){this.id=id;}

        private workSummary workSummary;
        public WorkRelationshipsAPIObject.workSummary getWorkSummary(){return workSummary;}
        public void setWorkSummary(WorkRelationshipsAPIObject.workSummary workSummary){this.workSummary=workSummary;}

        private String effectiveStartDate;
        public String getEffectiveStartDate() {return effectiveStartDate;}
        public void setEffectiveStartDate(String effectiveStartDate) {this.effectiveStartDate = effectiveStartDate;}

        private String  effectiveEndDate;
        public String getEffectiveEndDate() {return effectiveEndDate;}
        public void setEffectiveEndDate(String effectiveEndDate) {this.effectiveEndDate = effectiveEndDate;}
    }

    public static class workSummary {
        private String title;
        public void setTitle(String title) {this.title = title;}
        public String getTitle() {return title;}

        private HashMap<String,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}

        private HashMap<String,Object> status;
        public void setStatus(HashMap<String, Object> status) {this.status = status;}
        public HashMap<String, Object> getStatus() {return status;}


    }

    private void printLog(String verified){Log.info("verified..."+verified);}

}
