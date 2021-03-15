package com.eph.automation.testing.models.api;

import java.util.HashMap;

/**
 *created  by Nishant @ 09 Feb 2021, EPHD-2747
 */

public class ExtendedSubjectArea {

    private String code;
    public String getCode() {return code;}
    public void setCode(String code) {this.code = code;}

    private String name;
    public String getName() {return name;}
    public void setName(String name) {this.name = name;}

    private HashMap<String, Object> type;
    public HashMap<String, Object> getType() {return type;}
    public void setType(HashMap<String, Object> type) {this.type = type;}

    private String priority;
    public String getPriority() {return priority;}
    public void setPriority(String priority) {this.priority = priority;}
}
