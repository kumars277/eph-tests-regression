package com.eph.automation.testing.models.dao.StitchingExtended;
import java.util.HashMap;

public class WorkExtRelationshipsJson {

    private ExtendedSiblings[] extendedSibling;
    public ExtendedSiblings[] getExtendedSiblings() {return extendedSibling;}
    public void setExtendedSiblings(ExtendedSiblings[] extendedSibling) {this.extendedSibling = extendedSibling;}

    public static class ExtendedSiblings{
        private HashMap<String ,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}

        private String id;

        public String getId() {
            return id;}
        public void setId(String id) {this.id = id;}

        private WorkExtendedSummary workExtendedSummary;
        public WorkExtendedSummary getWorkExtendedSummary() {return workExtendedSummary;}
        public void setWorkExtendedSummary(WorkExtendedSummary workExtendedSummary) {this.workExtendedSummary = workExtendedSummary;}

        public static class WorkExtendedSummary{
            private String title;
            public String getTitle() {
                return title;}
            public void setTitle(String title) {this.title = title;}

            private HashMap<String ,Object> type;
            public HashMap<String, Object> getType() {return type;}
            public void setType(HashMap<String, Object> type) {this.type = type;}

            private HashMap<String ,Object> extendedStatus;
            public HashMap<String, Object> getExtendedStatus() {return extendedStatus;}
            public void setExtendedStatus(HashMap<String, Object> extendedStatus) {this.extendedStatus = extendedStatus;}

        }

    }
}




