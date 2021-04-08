package com.eph.automation.testing.models.dao.StitchingExtended;
import java.util.HashMap;

public class WorkExtSubAreaJson {




    private ExtendedSubArea extendedSubjectArea;
    public ExtendedSubArea getExtendedSubArea() {return extendedSubjectArea;}
    public void setExtendedSubArea(ExtendedSubArea extendedSubjectArea) {this.extendedSubjectArea = extendedSubjectArea;}

    public static class ExtendedSubArea{
        private HashMap<String ,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}

        private String code;
        private String name;

        public String getCode() {
            return code;}
        public void setCode(String code) {this.code = code;}

        public String getName() {
            return name;}
        public void setName(String name) {this.name = name;}

        private String priority;

        public String getPriority() {
            return priority;}
        public void setPriority(String priority) {this.priority = priority;}


    }
}




