package com.eph.automation.testing.models.api;

import java.util.HashMap;

//created by Nishant @16 Feb 2021, EPHD-2747
public class ProductSummary {

        String name;
        public String getName() {return name;}
        public void setName(String name) {this.name = name;}

        HashMap<String, Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}

        HashMap<String, Object> status;
        public HashMap<String, Object> getStatus() {return status;}
        public void setStatus(HashMap status) {this.status = status;}
    }
