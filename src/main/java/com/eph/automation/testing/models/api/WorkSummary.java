package com.eph.automation.testing.models.api;
//created by Nishant @14 Sep 2021, EPHD-3451
import java.util.HashMap;

public class WorkSummary {

        String title;
        public String getTitle() {return title;}
        public void setTitle(String title) {this.title = title;}

        HashMap<String, Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}

        HashMap<String, Object> status;
        public HashMap<String, Object> getStatus() {return status;}
        public void setStatus(HashMap status) {this.status = status;}


}
