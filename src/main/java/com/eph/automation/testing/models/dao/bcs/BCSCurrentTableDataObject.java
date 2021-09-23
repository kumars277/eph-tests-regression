//created by Nishant @ 21 Oct 2020
/*there are 14 current tables where data need to be verified.
for each table respective column variables are aligned.
any variable already used is commented and kept as is for reference.
*/

package com.eph.automation.testing.models.dao.bcs;


public class BCSCurrentTableDataObject {

    private String metadeleted;
    private String metamodifiedon;
    private String sourceref;

    //for stg_current_classification
    private String classificationcode;
    private String value;
    private String classificationtype;
    private String priority;
    private String businessunit;
    //for stg_current_content
    private String originimpid;
    private String subgroup;
    private String series;
    private String copyrightyear;
    private String title;
    private String worktitle;
    private String isset;
    private String impressionid;
    private String doistatus;
    private String imprint;
    private String division;
    private String language3;
    private String regstatus;
    private String approvedondate;
    private String language2;
    private String doi;
    private String volumeno;
    private String editionid;
    private String synctemplate;
    private String volumename;
    private String seriesissn;
    private String seriesid;
    private String ownership;
    private String firstapproval;
    private String companygroup;
    private String shorttitle;
    private String editionno;
    private String work_master_flag;
    private String language;
    private String piidack;
    private String publisher;
    private String titleid;
    private String subtitle;
    private String seriescode;
    private String objecttype;

    //for stg_current_extobject
    private String object;
    private String type;
    private String name;
    private String comments;
    private String source;

    //for stg_current_fullversionfamily
    private String versiontype;
    private String isbn;
    private String projectno;
    private String workmaster;

    //for stg_current_originatoraddress
    private String additionaladdress;
    private String addressid;
    private String addressline1;
    private String addressline2;
    private String addressline3;
    private String businesspartnerid;
    private String city;
    private String country;
    private String district;
    private String email;
    private String fax;
    private String houseno;
    private String internet;
    private String ismainaddress;
    private String mobile;
    private String postalcode;
    private String street;
    private String telephonemain;
    private String telephoneother;

    //for stg_current_originators
    //private String metadeleted;
    //private String metamodifiedon;
    //private String sourceref;
    //private String businesspartnerid;
    private String prefix;
    private String sequence;
    private String originatorid;
    private String isperson;
    private String locationid;
    private String copyrightholdertype;
    private String institution;
    private String firstname;
    private String department;
    private String lastname;
    private String searchterm;

    //for stg_current_pricing
    //private String metadeleted;
    //private String metamodifiedon;
    //private String sourceref;
    //private String type;
    private String validfrom;
    private String currency;
    private String priceapprox;
    private String price;
    private String validto;

    //for stg_current_product
    //private String isbn;
    //private String metadeleted;
    //private String metamodifiedon;
    //private String projectno;
    //private String sourceref;
    //private String versiontype;
    private	String	ausavailablestock;
    private	String	binding;
    private	String	budgetpubdate;
    private	String	contractpubdate;
    private	String	createdon;
    private	String	deliverystatus;
    private	String	externaleditionid;
    private	String	externalimpressionid;
    private	String	firstprinting;
    private	String	firstrelease;
    private	String	fraavailablestock;
    private	String	fratotalstock;
    private	String	geravailablestock;
    private	String	gertotalstock;
    private	String	isbn13;
    private	String	latestpubdate;
    private	String	medium;
    private	String	modifiedon;
    private	String	noofvolumes;
    private	String	orderno;
    private	String	origtitle;
    private	String	planned;
    private	String	plannededitionsize;
    private	String	plannedfirstprint;
    private	String	podsuitable;
    private	String	pubdateplanned;
    private	String	publishedon;
    private	String	reason;
    private	String	refkey;
    private	String	ukavailablestock;
    private	String	uktotalstock;
    private	String	unitcost;
    private	String	usavailablestock;
    private	String	ustotalstock;

    //for stg_current_production
    //private String metadeleted;
    //private String metamodifiedon;
    //private String sourceref;
    private String addillustration;
    private String approxpages;
    private String authoringsystem;
    private String backcoverpms;
    private String backpapercolour;
    private String biblioreference;
    private String bindmeth;
    private String boardthickness;
    private String classification;
    private String copyedlevel;
    private String duotone;
    private String eonlypages;
    private String extentprod;
    private String extentstatus;
    private String exteriorpms;
    private String externalads;
    private String finishing;
    private String format;
    private String frontcoverpms;
    private String frontpapercolour;
    private String grammage;
    //private String grammage;
    private String graphicsbw;
    private String graphicscolors;
    private String halftonesbw;
    private String halftonescolors;
    private String illustrationsbw;
    private String illustrationscolors;
    private String interiorcolour;
    private String interiorpms;
    private String internalads;
    private String lineartbw;
    private String lineartcolors;
    private String manuscriptpages;
    private String mapsbw;
    private String mapscolor;
    private String material;
    private String mstype;
    private String pagesarabic;
    private String pagesroman;
    private String paperquality;
    private String printform;
    private String productiondetails;
    private String productionmethod;
    private String reprintno;
    private String sectioncolours;
    private String spec;
    private String spinestyle;
    private String suppliera;
    private String supplierafullname;
    private String supplierashortname;
    private String supplierb;
    private String supplierbfullname;
    private String supplierbshortname;
    private String tablesbw;
    private String tablescolors;
    private String tagging;
    private String textdesigntype;
    private String trimother;
    private String trimsize;
    private String weight;

    //for stg_current_relations
    //private String metadeleted;
    //private String metamodifiedon;
    //private String sourceref;
    //private String orderno;
    //private String projectno;
    //private String relimpressionid;
    //private String isbn;
    private String relationtype;

    //for stg_current_responsibilities
    //private String metadeleted;
    //private String metamodifiedon;
    //private String sourceref;
    private String responsibility;
    private String responsibleperson;
    private String personid;

    //for stg_current_sublocation
    //private String metadeleted;
    //private String metamodifiedon;
    //private String sourceref;
    //private String refkey;
    private String warehouse;
    private String pubdateactual;
    private String stocksplit;
    private String status;
    private String plannedpubdate;


    //for stg_current_text
    //private String metadeleted;
    //private String metamodifiedon;
    //private String sourceref;
    //private String name;
    //private String status;
    private String tab;
    private String texttype;
    private String text;

    //for current versionfamily
    //private String metadeleted;
    //private String metamodifiedon;
    //private String sourceref;
    private String workmasterisbn;
    private String workmasterprojectno;
    private String childisbn;
    private String childprojectno;

    //for stg_current_originatornotes
    private String notestype;
    private String notes;


    public String getNotestype() {return notestype;}
    public void setNotestype(String notestype) {this.notestype = notestype;}

    public String getNotes() {return notes;}
    public void setNotes(String notes) {this.notes = notes;}




    public String getClassificationcode() {return classificationcode;}
    public void setClassificationcode(String classificationcode) {this.classificationcode = classificationcode;}

    public String getClassificationtype() {return classificationtype;}
    public void setClassificationtype(String classificationtype) {this.classificationtype = classificationtype;}

    public String getMetadeleted() {return metadeleted;}
    public void setMetadeleted(String metadeleted) {this.metadeleted = metadeleted;}

    public String getMetamodifiedon() {return metamodifiedon;}
    public void setMetamodifiedon(String metamodifiedon) {this.metamodifiedon = metamodifiedon;}

    public String getBusinessunit() {return businessunit;}
    public void setBusinessunit(String businessunit) {this.businessunit = businessunit;}

    public String getPriority() {return priority;}
    public void setPriority(String priority) {this.priority = priority;}

    public String getSourceref() {return sourceref;}
    public void setSourceref(String sourceref) {this.sourceref = sourceref;}

    public String getValue() {return value;}
    public void setValue(String value) {this.value = value;}


    public String getOriginimpid() {return originimpid;}
    public void setOriginimpid(String originimpid) {this.originimpid = originimpid;}

    public String getSubgroup() {return subgroup;}
    public void setSubgroup(String subgroup) {this.subgroup = subgroup;}

    public String getSeries() {return series;}
    public void setSeries(String series) {this.series = series;}

    public String getSeriesissn() {return seriesissn;}
    public void setSeriesissn(String seriesissn) {this.seriesissn = seriesissn;}

    public String getSeriescode() {return seriescode;}
    public void setSeriescode(String seriescode) {this.seriescode = seriescode;}

    public String getSeriesid() {return seriesid;}
    public void setSeriesid(String seriesid) {this.seriesid = seriesid;}

    public String getCopyrightyear() {return copyrightyear;}
    public void setCopyrightyear(String copyrightyear) {this.copyrightyear = copyrightyear;}

    public String getTitle() {return title;}
    public void setTitle(String title) {this.title = title;}

    public String getTitleid() {return titleid;}
    public void setTitleid(String titleid) {this.titleid = titleid;}

    public String getSubtitle() {return subtitle;}
    public void setSubtitle(String subtitle) {this.subtitle = subtitle;}

    public String getWorktitle() {return worktitle;}
    public void setWorktitle(String worktitle) {this.worktitle = worktitle;}

    public String getWork_master_flag() {return work_master_flag;}
    public void setWork_master_flag(String work_master_flag) {this.work_master_flag = work_master_flag;}

    public String getIsset() {return isset;}
    public void setIsset(String isset) {this.isset = isset;}

    public String getImpressionid() {return impressionid;}
    public void setImpressionid(String impressionid) {this.impressionid = impressionid;}

    public String getDoistatus() {return doistatus;}
    public void setDoistatus(String doistatus) {this.doistatus = doistatus;}

    public String getImprint() {return imprint;}
    public void setImprint(String imprint) {this.imprint = imprint;}

    public String getDivision() {return division;}
    public void setDivision(String division) {this.division = division;}

    public String getLanguage() {return language;}
    public void setLanguage(String language) {this.language = language;}

    public String getLanguage2() {return language2;}
    public void setLanguage2(String language2) {this.language2 = language2;}

    public String getLanguage3() {return language3;}
    public void setLanguage3(String language3) {this.language3 = language3;}

    public String getRegstatus() {return regstatus;}
    public void setRegstatus(String regstatus) {this.regstatus = regstatus;}

    public String getApprovedondate() {return approvedondate;}
    public void setApprovedondate(String approvedondate) {this.approvedondate = approvedondate;}

    public String getDoi() {return doi;}
    public void setDoi(String doi) {this.doi = doi;}

    public String getVolumeno() {return volumeno;}
    public void setVolumeno(String volumeno) {this.volumeno = volumeno;}

    public String getVolumename() {return volumename;}
    public void setVolumename(String volumename) {this.volumename = volumename;}

    public String getEditionid() {return editionid;}
    public void setEditionid(String editionid) {this.editionid = editionid;}

    public String getEditionno() {return editionno;}
    public void setEditionno(String editionno) {this.editionno = editionno;}

    public String getSynctemplate() {return synctemplate;}
    public void setSynctemplate(String synctemplate) {this.synctemplate = synctemplate;}

    public String getOwnership() {return ownership;}
    public void setOwnership(String ownership) {this.ownership = ownership;}

    public String getFirstapproval() {return firstapproval;}
    public void setFirstapproval(String firstapproval) {this.firstapproval = firstapproval;}

    public String getCompanygroup() {return companygroup;}
    public void setCompanygroup(String companygroup) {this.companygroup = companygroup;}

    public String getShorttitle() {return shorttitle;}
    public void setShorttitle(String shorttitle) {this.shorttitle = shorttitle;}

    public String getPiidack() {return piidack;}
    public void setPiidack(String piidack) {this.piidack = piidack;}

    public String getPublisher() {return publisher;}
    public void setPublisher(String publisher) {this.publisher = publisher;}

    public String getObjecttype() {return objecttype;}
    public void setObjecttype(String objecttype) {this.objecttype = objecttype;}

    public String getComments() {return comments;}
    public void setComments(String comments) {this.comments = comments;}

    public String getName() {return name;}
    public void setName(String name) {this.name = name;}

    public String getObject() {return object;}
    public void setObject(String object) {this.object = object;}

    public String getType() {return type;}
    public void setType(String type) {this.type = type;}

    public String getSource() {return source;}
    public void setSource(String source) {this.source = source;}


    public String getVersiontype() {return versiontype;}
    public void setVersiontype(String versiontype) {this.versiontype = versiontype;}

    public String getIsbn() {return isbn;}
    public void setIsbn(String isbn) {this.isbn = isbn;}

    public String getProjectno() {return projectno;}
    public void setProjectno(String projectno) {this.projectno = projectno;}

    public String getWorkmaster() {return workmaster;}
    public void setWorkmaster(String workmaster) {this.workmaster = workmaster;}


    public String getAdditionaladdress() {return additionaladdress;}
    public void setAdditionaladdress(String additionaladdress) {this.additionaladdress = additionaladdress;}

    public String getAddressid() {return addressid;}
    public void setAddressid(String addressid) {this.addressid = addressid;}

    public String getAddressline1() {return addressline1;}
    public void setAddressline1(String addressline1) {this.addressline1 = addressline1;}

    public String getAddressline2() {return addressline2;}
    public void setAddressline2(String addressline2) {this.addressline2 = addressline2;}

    public String getAddressline3() {return addressline3;}
    public void setAddressline3(String addressline3) {this.addressline3 = addressline3;}

    public String getBusinesspartnerid() {return businesspartnerid;}
    public void setBusinesspartnerid(String businesspartnerid) {this.businesspartnerid = businesspartnerid;}

    public String getCity() {return city;}
    public void setCity(String city) {this.city = city;}

    public String getCountry() {return country;}
    public void setCountry(String country) {this.country = country;}

    public String getDistrict() {return district;}
    public void setDistrict(String district) {this.district = district;}

    public String getEmail() {return email;}
    public void setEmail(String email) {this.email = email;}

    public String getFax() {return fax;}
    public void setFax(String fax) {this.fax = fax;}

    public String getHouseno() {return houseno;}
    public void setHouseno(String houseno) {this.houseno = houseno;}

    public String getInternet() {return internet;}
    public void setInternet(String internet) {this.internet = internet;}

    public String getIsmainaddress() {return ismainaddress;}
    public void setIsmainaddress(String ismainaddress) {this.ismainaddress = ismainaddress;}

    public String getMobile() {return mobile;}
    public void setMobile(String mobile) {this.mobile = mobile;}

    public String getPostalcode() {return postalcode;}
    public void setPostalcode(String postalcode) {this.postalcode = postalcode;    }

    public String getStreet() {return street;}
    public void setStreet(String street) {this.street = street;}

    public String getTelephonemain() {return telephonemain;}
    public void setTelephonemain(String telephonemain) {this.telephonemain = telephonemain;}

    public String getTelephoneother() {return telephoneother;}
    public void setTelephoneother(String telephoneother) {this.telephoneother = telephoneother;}


    public String getPrefix() {return prefix;}
    public void setPrefix(String prefix) {this.prefix = prefix;}

    public String getSequence() {return sequence;}
    public void setSequence(String sequence) {this.sequence = sequence;}

    public String getOriginatorid() {return originatorid;}
    public void setOriginatorid(String originatorid) {this.originatorid = originatorid;}

    public String getIsperson() {return isperson;}
    public void setIsperson(String isperson) {this.isperson = isperson;}

    public String getLocationid() {return locationid;}
    public void setLocationid(String locationid) {this.locationid = locationid;}

    public String getCopyrightholdertype() {return copyrightholdertype;}
    public void setCopyrightholdertype(String copyrightholdertype) {this.copyrightholdertype = copyrightholdertype;}

    public String getInstitution() {return institution;}
    public void setInstitution(String institution) {this.institution = institution;}

    public String getFirstname() {return firstname;}
    public void setFirstname(String firstname) {this.firstname = firstname;}

    public String getDepartment() {return department;}
    public void setDepartment(String department) {this.department = department;}

    public String getLastname() {return lastname;}
    public void setLastname(String lastname) {this.lastname = lastname;}

    public String getSearchterm() {return searchterm;}
    public void setSearchterm(String searchterm) {this.searchterm = searchterm;}

    public String getCurrency() {return currency;}
    public void setCurrency(String currency) {this.currency = currency;}

    public String getPrice() {return price;}
    public void setPrice(String price) {this.price = price;}

    public String getPriceapprox() {return priceapprox;}
    public void setPriceapprox(String priceapprox) {this.priceapprox = priceapprox;}

    public String getValidfrom() {return validfrom;}
    public void setValidfrom(String validfrom) {this.validfrom = validfrom;}

    public String getValidto() {return validto;}
    public void setValidto(String validto) {this.validto = validto;}


    public String getAusavailablestock() {return ausavailablestock;}
    public void setAusavailablestock(String ausavailablestock) {this.ausavailablestock = ausavailablestock;}

    public String getBinding() {return binding;   }
    public void setBinding(String binding) {this.binding = binding;}

    public String getBudgetpubdate() {return budgetpubdate;}
    public void setBudgetpubdate(String budgetpubdate) {this.budgetpubdate = budgetpubdate;}

    public String getContractpubdate() {return contractpubdate;}
    public void setContractpubdate(String contractpubdate) {this.contractpubdate = contractpubdate;}

    public String getCreatedon() {return createdon;}
    public void setCreatedon(String createdon) {this.createdon = createdon;}

    public String getDeliverystatus() {return deliverystatus;}
    public void setDeliverystatus(String deliverystatus) {this.deliverystatus = deliverystatus;}

    public String getExternaleditionid() {return externaleditionid;}
    public void setExternaleditionid(String externaleditionid) {this.externaleditionid = externaleditionid;}

    public String getExternalimpressionid() {return externalimpressionid;}
    public void setExternalimpressionid(String externalimpressionid) {this.externalimpressionid = externalimpressionid;}

    public String getFirstprinting() {return firstprinting;}
    public void setFirstprinting(String firstprinting) {this.firstprinting = firstprinting;}

    public String getFirstrelease() {return firstrelease;}
    public void setFirstrelease(String firstrelease) {this.firstrelease = firstrelease;}

    public String getFraavailablestock() {return fraavailablestock;}
    public void setFraavailablestock(String fraavailablestock) {this.fraavailablestock = fraavailablestock;}

    public String getFratotalstock() {return fratotalstock;}
    public void setFratotalstock(String fratotalstock) {this.fratotalstock = fratotalstock;}

    public String getGeravailablestock() {return geravailablestock;}
    public void setGeravailablestock(String geravailablestock) {this.geravailablestock = geravailablestock;}

    public String getGertotalstock() {return gertotalstock;}
    public void setGertotalstock(String gertotalstock) {this.gertotalstock = gertotalstock;}

    public String getIsbn13() {return isbn13;}
    public void setIsbn13(String isbn13) {this.isbn13 = isbn13;}

    public String getLatestpubdate() {return latestpubdate;}
    public void setLatestpubdate(String latestpubdate) {this.latestpubdate = latestpubdate;}

    public String getMedium() {return medium;}
    public void setMedium(String medium) {this.medium = medium;}

    public String getModifiedon() {return modifiedon;}
    public void setModifiedon(String modifiedon) {this.modifiedon = modifiedon;}

    public String getNoofvolumes() {return noofvolumes;}
    public void setNoofvolumes(String noofvolumes) {this.noofvolumes = noofvolumes;}

    public String getOrderno() {return orderno;}
    public void setOrderno(String orderno) {this.orderno = orderno;}

    public String getOrigtitle() {return origtitle;}
    public void setOrigtitle(String origtitle) {this.origtitle = origtitle;}

    public String getPlanned() {return planned;}
    public void setPlanned(String planned) {this.planned = planned;}

    public String getPlannededitionsize() {return plannededitionsize;}
    public void setPlannededitionsize(String plannededitionsize) {this.plannededitionsize = plannededitionsize;}

    public String getPlannedfirstprint() {return plannedfirstprint;   }
    public void setPlannedfirstprint(String plannedfirstprint) {this.plannedfirstprint = plannedfirstprint;}

    public String getPodsuitable() {return podsuitable;}
    public void setPodsuitable(String podsuitable) {this.podsuitable = podsuitable;}

    public String getPubdateplanned() {return pubdateplanned;}
    public void setPubdateplanned(String pubdateplanned) {this.pubdateplanned = pubdateplanned;}

    public String getPublishedon() {return publishedon;}
    public void setPublishedon(String publishedon) {this.publishedon = publishedon;}

    public String getReason() {return reason;}
    public void setReason(String reason) {this.reason = reason;}

    public String getRefkey() {return refkey;}
    public void setRefkey(String refkey) {this.refkey = refkey;}

    public String getUkavailablestock() {return ukavailablestock;}
    public void setUkavailablestock(String ukavailablestock) {this.ukavailablestock = ukavailablestock;}

    public String getUktotalstock() {return uktotalstock;}
    public void setUktotalstock(String uktotalstock) {this.uktotalstock = uktotalstock;}

    public String getUnitcost() {return unitcost;}
    public void setUnitcost(String unitcost) {this.unitcost = unitcost;}

    public String getUsavailablestock() {return usavailablestock;}
    public void setUsavailablestock(String usavailablestock) {this.usavailablestock = usavailablestock;}

    public String getUstotalstock() {return ustotalstock;}
    public void setUstotalstock(String ustotalstock) {this.ustotalstock = ustotalstock;}

    public String getAddillustration() {return addillustration;}
    public void setAddillustration(String addillustration) {this.addillustration = addillustration;}

    public String getApproxpages() {return approxpages;}
    public void setApproxpages(String approxpages) {this.approxpages = approxpages;}

    public String getAuthoringsystem() {return authoringsystem;}
    public void setAuthoringsystem(String authoringsystem) {this.authoringsystem = authoringsystem;}

    public String getBackcoverpms() {return backcoverpms;}
    public void setBackcoverpms(String backcoverpms) {this.backcoverpms = backcoverpms;}

    public String getBackpapercolour() {return backpapercolour;}
    public void setBackpapercolour(String backpapercolour) {this.backpapercolour = backpapercolour;}

    public String getBiblioreference() {return biblioreference;}
    public void setBiblioreference(String biblioreference) {this.biblioreference = biblioreference;}

    public String getBindmeth() {return bindmeth;}
    public void setBindmeth(String bindmeth) {this.bindmeth = bindmeth;}

    public String getBoardthickness() {return boardthickness;}
    public void setBoardthickness(String boardthickness) {this.boardthickness = boardthickness;}

    public String getClassification() {return classification;}
    public void setClassification(String classification) {this.classification = classification;}

    public String getCopyedlevel() {return copyedlevel;}
    public void setCopyedlevel(String copyedlevel) {this.copyedlevel = copyedlevel;}

    public String getDuotone() {return duotone;}
    public void setDuotone(String duotone) {this.duotone = duotone;}

    public String getEonlypages() {return eonlypages;}
    public void setEonlypages(String eonlypages) {this.eonlypages = eonlypages;}

    public String getExtentprod() {return extentprod;}
    public void setExtentprod(String extentprod) {this.extentprod = extentprod;}

    public String getExtentstatus() {return extentstatus;}
    public void setExtentstatus(String extentstatus) {this.extentstatus = extentstatus;}

    public String getExteriorpms() {return exteriorpms;}
    public void setExteriorpms(String exteriorpms) {this.exteriorpms = exteriorpms;}

    public String getExternalads() {return externalads;}
    public void setExternalads(String externalads) {this.externalads = externalads;}

    public String getFinishing() {return finishing;}
    public void setFinishing(String finishing) {this.finishing = finishing;}

    public String getFormat() {return format;}
    public void setFormat(String format) {this.format = format;}

    public String getFrontcoverpms() {return frontcoverpms;}
    public void setFrontcoverpms(String frontcoverpms) {this.frontcoverpms = frontcoverpms;}

    public String getFrontpapercolour() {return frontpapercolour;}
    public void setFrontpapercolour(String frontpapercolour) {this.frontpapercolour = frontpapercolour;}

    public String getGrammage() {return grammage;}
    public void setGrammage(String grammage) {this.grammage = grammage;}

    public String getGraphicsbw() {return graphicsbw;}
    public void setGraphicsbw(String graphicsbw) {this.graphicsbw = graphicsbw;}

    public String getGraphicscolors() {return graphicscolors;}
    public void setGraphicscolors(String graphicscolors) {this.graphicscolors = graphicscolors;}

    public String getHalftonesbw() {return halftonesbw;}
    public void setHalftonesbw(String halftonesbw) {this.halftonesbw = halftonesbw;}

    public String getHalftonescolors() {return halftonescolors;}
    public void setHalftonescolors(String halftonescolors) {this.halftonescolors = halftonescolors;}

    public String getIllustrationsbw() {return illustrationsbw;}
    public void setIllustrationsbw(String illustrationsbw) {this.illustrationsbw = illustrationsbw;}

    public String getIllustrationscolors() {return illustrationscolors;}
    public void setIllustrationscolors(String illustrationscolors) {this.illustrationscolors = illustrationscolors;}

    public String getInteriorcolour() {return interiorcolour;}
    public void setInteriorcolour(String interiorcolour) {this.interiorcolour = interiorcolour;}

    public String getInteriorpms() {return interiorpms;}
    public void setInteriorpms(String interiorpms) {this.interiorpms = interiorpms;}

    public String getInternalads() {return internalads;}
    public void setInternalads(String internalads) {this.internalads = internalads;}

    public String getLineartbw() {return lineartbw;}
    public void setLineartbw(String lineartbw) {this.lineartbw = lineartbw;}

    public String getLineartcolors() {return lineartcolors;}
    public void setLineartcolors(String lineartcolors) {this.lineartcolors = lineartcolors;}

    public String getManuscriptpages() {return manuscriptpages;}
    public void setManuscriptpages(String manuscriptpages) {this.manuscriptpages = manuscriptpages;}

    public String getMapsbw() {return mapsbw;}
    public void setMapsbw(String mapsbw) {this.mapsbw = mapsbw;}

    public String getMapscolor() {return mapscolor;}
    public void setMapscolor(String mapscolor) {this.mapscolor = mapscolor;}

    public String getMaterial() {return material;}
    public void setMaterial(String material) {this.material = material;}

    public String getMstype() {return mstype;}
    public void setMstype(String mstype) {this.mstype = mstype;}

    public String getPagesarabic() {return pagesarabic;}
    public void setPagesarabic(String pagesarabic) {this.pagesarabic = pagesarabic;}

    public String getPagesroman() {return pagesroman;}
    public void setPagesroman(String pagesroman) {this.pagesroman = pagesroman;}

    public String getPaperquality() {return paperquality;}
    public void setPaperquality(String paperquality) {this.paperquality = paperquality;}

    public String getPrintform() {return printform;}
    public void setPrintform(String printform) {this.printform = printform;}

    public String getProductiondetails() {return productiondetails;}
    public void setProductiondetails(String productiondetails) {this.productiondetails = productiondetails;}

    public String getProductionmethod() {return productionmethod;}
    public void setProductionmethod(String productionmethod) {this.productionmethod = productionmethod;}

    public String getReprintno() {return reprintno;}
    public void setReprintno(String reprintno) {this.reprintno = reprintno;}

    public String getSectioncolours() {return sectioncolours;}
    public void setSectioncolours(String sectioncolours) {this.sectioncolours = sectioncolours;}

    public String getSpec() {return spec;}
    public void setSpec(String spec) {this.spec = spec;}

    public String getSpinestyle() {return spinestyle;}
    public void setSpinestyle(String spinestyle) {this.spinestyle = spinestyle;}

    public String getSuppliera() {return suppliera;}
    public void setSuppliera(String suppliera) {this.suppliera = suppliera;}

    public String getSupplierafullname() {return supplierafullname;}
    public void setSupplierafullname(String supplierafullname) {this.supplierafullname = supplierafullname;}

    public String getSupplierashortname() {return supplierashortname;}
    public void setSupplierashortname(String supplierashortname) {this.supplierashortname = supplierashortname;}

    public String getSupplierb() {return supplierb;}
    public void setSupplierb(String supplierb) {this.supplierb = supplierb;}

    public String getSupplierbfullname() {return supplierbfullname;}
    public void setSupplierbfullname(String supplierbfullname) {this.supplierbfullname = supplierbfullname;}

    public String getSupplierbshortname() {return supplierbshortname;}
    public void setSupplierbshortname(String supplierbshortname) {this.supplierbshortname = supplierbshortname;}

    public String getTablesbw() {return tablesbw;}
    public void setTablesbw(String tablesbw) {this.tablesbw = tablesbw;}

    public String getTablescolors() {return tablescolors;}
    public void setTablescolors(String tablescolors) {this.tablescolors = tablescolors;}

    public String getTagging() {return tagging;}
    public void setTagging(String tagging) {this.tagging = tagging;}

    public String getTextdesigntype() {return textdesigntype;}
    public void setTextdesigntype(String textdesigntype) {this.textdesigntype = textdesigntype;}

    public String getTrimother() {return trimother;}
    public void setTrimother(String trimother) {this.trimother = trimother;}

    public String getTrimsize() {return trimsize;}
    public void setTrimsize(String trimsize) {this.trimsize = trimsize;}

    public String getWeight() {return weight;}
    public void setWeight(String weight) {this.weight = weight;}

    public String getRelationtype() {return relationtype;}
    public void setRelationtype(String relationtype) {this.relationtype = relationtype;}

    public String getPersonid() {return personid;}
    public void setPersonid(String personid) {this.personid = personid;}

    public String getResponsibility() {return responsibility;}
    public void setResponsibility(String responsibility) {this.responsibility = responsibility;}

    public String getResponsibleperson() {return responsibleperson;}
    public void setResponsibleperson(String responsibleperson) {this.responsibleperson = responsibleperson;}

    public String getPlannedpubdate() {return plannedpubdate;}
    public void setPlannedpubdate(String plannedpubdate) {this.plannedpubdate = plannedpubdate;}

    public String getPubdateactual() {return pubdateactual;}
    public void setPubdateactual(String pubdateactual) {this.pubdateactual = pubdateactual;}

    public String getStatus() {return status;}
    public void setStatus(String status) {this.status = status;}

    public String getStocksplit() {return stocksplit;}
    public void setStocksplit(String stocksplit) {this.stocksplit = stocksplit;}

    public String getWarehouse() {return warehouse;}
    public void setWarehouse(String warehouse) {this.warehouse = warehouse;}

    public String getTab() {return tab;}
    public void setTab(String tab) {this.tab = tab;}

    public String getText() {return text;}
    public void setText(String text) {this.text = text;}

    public String getTexttype() {return texttype;}
    public void setTexttype(String texttype) {this.texttype = texttype;}


    public String getChildisbn() {return childisbn;}
    public void setChildisbn(String childisbn) {this.childisbn = childisbn;}

    public String getChildprojectno() {return childprojectno;}
    public void setChildprojectno(String childprojectno) {this.childprojectno = childprojectno;}

    public String getWorkmasterisbn() {return workmasterisbn;}
    public void setWorkmasterisbn(String workmasterisbn) {this.workmasterisbn = workmasterisbn;}

    public String getWorkmasterprojectno() {return workmasterprojectno;}
    public void setWorkmasterprojectno(String workmasterprojectno) {this.workmasterprojectno = workmasterprojectno;}

}
