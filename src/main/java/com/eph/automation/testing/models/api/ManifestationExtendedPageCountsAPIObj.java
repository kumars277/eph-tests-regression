package com.eph.automation.testing.models.api;

import java.util.HashMap;

//created by Nishant @ 05 Feb 2021, EPHD-2747
public class ManifestationExtendedPageCountsAPIObj {

    private ExtendedPageCount extendedPageCount;
    public ExtendedPageCount getExtendedPageCount() {return extendedPageCount;}
    public void setExtendedPageCount(ExtendedPageCount extendedPageCount) {this.extendedPageCount = extendedPageCount;}

    public class ExtendedPageCount{

        private HashMap<String,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}

        private String count;
        public String getCount() {return count;}
        public void setCount(String count) {this.count = count;}
    }

}
