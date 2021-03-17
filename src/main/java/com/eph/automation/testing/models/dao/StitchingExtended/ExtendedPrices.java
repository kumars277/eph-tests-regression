package com.eph.automation.testing.models.dao.StitchingExtended;

public class ExtendedPrices {



    private String currency;
    private String  amount;
    private String startDate;
    private String category;
    private String endDate;
    private String region;
    private String customerCategory;
    private String purchaseQuantity;


    public String getCurrency() { return currency;  }
    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getPurchaseQuantity() { return purchaseQuantity;  }
    public void setPurchaseQuantity(String purchaseQuantity) {
        this.purchaseQuantity = purchaseQuantity;
    }

    public String getCustomerCategory() { return customerCategory;  }
    public void setCustomerCategory(String customerCategory) {
        this.customerCategory = customerCategory;
    }

    public String getCategory() { return category;  }
    public void setCategory(String category) {
        this.category = category;
    }

    public String getEndDate() { return endDate;  }
    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getRegion() { return region;  }
    public void setRegion(String region) {
        this.region = region;
    }

    public String getStartDate() { return startDate;  }
    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getAmount() { return amount;  }
    public void setAmount(String amount) {
        this.amount = amount;
    }



    /*
    private String applicationName;
    public String getApplicationName() {return applicationName;}
    public void setApplicationName(String applicationName) {this.applicationName = applicationName;}
*/
   /* public static class ExtendedPageCount{
        private HashMap<String ,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}


        private String count;
        public String getCount() {
            return count;}
        public void setCount(String count) {this.count = count;}
    }*/


}

