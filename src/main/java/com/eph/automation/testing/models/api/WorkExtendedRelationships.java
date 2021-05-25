package com.eph.automation.testing.models.api;
//created by Nishant @ 25 May 2021 EPHD-3122

import java.util.HashMap;

public class WorkExtendedRelationships {


    public class ExtendedSibling
    {
        private HashMap<String,String> type;
        public HashMap<String, String> getType() {return type;}
        public void setType(HashMap<String, String> type) {this.type = type;}

        private String id;
        public String getId() {return id;}
        public void setId(String id) {this.id = id;}



    }

    public class WorkExtendedSummary{


    }

}
