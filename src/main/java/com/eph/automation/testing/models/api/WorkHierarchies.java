package com.eph.automation.testing.models.api;
//added by Nishant @ 19 Oct 2021

import java.util.HashMap;

public class WorkHierarchies {

    private Hierarchy hierarchy;
    public Hierarchy getHierarchy() {return hierarchy;}
    public void setHierarchy(Hierarchy hierarchy) {this.hierarchy = hierarchy;}

    private String effectiveStartDate;
    public String getEffectiveStartDate() {return effectiveStartDate;}
    public void setEffectiveStartDate(String effectiveStartDate) {this.effectiveStartDate = effectiveStartDate;}

    public static class Hierarchy
    {
        private String hierarchyLevel;
        public String getHierarchyLevel() {return hierarchyLevel;}
        public void setHierarchyLevel(String hierarchyLevel) {this.hierarchyLevel = hierarchyLevel;}

        private HashMap<String, Object> hierarchyType;
        public HashMap<String, Object> getHierarchyType() {return hierarchyType;}
        public void setHierarchyType(HashMap<String, Object> hierarchyType) {this.hierarchyType = hierarchyType;}

        private String code;
        public String getCode() {return code;}
        public void setCode(String code) {this.code = code;}

        private String name;
        public String getName() {return name;}
        public void setName(String name) {this.name = name;}

        private HashMap<String, Object> parentHierarchy;
        public HashMap<String, Object> getParentHierarchy() {return parentHierarchy;}
        public void setParentHierarchy(HashMap<String, Object> parentHierarchy) {this.parentHierarchy = parentHierarchy;}

        private String effectiveStartDate;
        public String getEffectiveStartDate() {return effectiveStartDate;}
        public void setEffectiveStartDate(String effectiveStartDate) {this.effectiveStartDate = effectiveStartDate;}
    }
}
