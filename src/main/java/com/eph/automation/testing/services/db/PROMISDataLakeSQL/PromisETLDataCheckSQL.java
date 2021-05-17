package com.eph.automation.testing.services.db.PROMISDataLakeSQL;

public class PromisETLDataCheckSQL {
    public static String GET_PRMAUTPUBT_IDS = "select pub_idt as PUB_IDT from "+GetPRMDLDBUser.getPRMDataBase()+".%s\n"+
        "where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts limit 1) order by rand() limit %s";

    public static String GET_PRMCLSCODT_IDS = "select cls_cod as CLS_COD from "+GetPRMDLDBUser.getPRMDataBase()+".%s\n"+
            "where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts limit 1) order by rand() limit %s";

    public static String GET_PRMPUBRELT_IDS = "select pub_pub_idt as PUB_PUB_IDT from "+GetPRMDLDBUser.getPRMDataBase()+".%s\n"+
            "where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts limit 1) order by rand() limit %s";

    public static String GET_MKT_IDT_IDS = "select mkt_idt as MKT_IDT from "+GetPRMDLDBUser.getPRMDataBase()+".%s\n"+
            "where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts limit 1) order by rand() limit %s";

    public static String GET_UKEY_IDS = "select u_key as U_KEY from "+GetPRMDLDBUser.getPRMDataBase()+ ".%s order by rand() limit %s";

    public static String GET_UKEY_IDS_AND_PUB_IDT = "select u_key as U_KEY, pub_idt as PUB_IDT from "+GetPRMDLDBUser.getPRMDataBase()+ ".%s order by rand() limit %s";

    public static String GET_PUB_IDT_IDS = "select pub_idt as PUB_IDT from "+GetPRMDLDBUser.getPRMDataBase()+ ".%s order by rand() limit %s";

    public static String GET_Promis_prmautpubt_part = "select "
            + "ppa.pub_idt"
            +  ", ppa.aut_edt_idt"
            + ", cast(ppa.aut_edt_typ as integer) as aut_edt_typ"
            + ", ppa.typ_des"
            + ", cast(ppa.seq_num as integer) as seq_num"
            + ",ppa.aut_edt_pre"
            + ", ppa.aut_edt_ini"
            + ", ppa.aut_edt_nam"
            + ", ppa.aut_edt_sort"
            + ", ppa.aut_edt_suf"
            + ", ppa.aff_txt"
            + ", ppa.ftn"
            + ", ppa.aut_edt_jci"
            + ", ppa.bio_image"
            + ", ppa.inbound_ts "
            +"FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s ppa\n" +
            " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND PUB_IDT in (%s) order by AUT_EDT_IDT";

    public static String GET_Promis_prmclscodt_part = "select\n" +
            "  cls_cod,\n" +
            "  cls_des,\n" +
            "  inbound_ts\n" +
            " FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s\n" +
            " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND CLS_COD in ('%s')";

    public static String GET_Promis_prmclst_part = "SELECT " +
            "ppp.pub_idt \n" +
            ",ppp.cls_cod \n" +
            ",cast(ppp.sca_cod as integer) priority \n" +
            ", ppp.inbound_ts \n" +
            " FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s ppp\n" +
            " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND PUB_IDT in (%s) order by CLS_COD";

    public static String GET_Promis_prmlondest_part = "SELECT\n" +
            " ppp.pub_idt \n" +
            ", ppp.pub_vol_idt \n" +
            ", ppp.vol_prt_idt \n" +
            ", ppp.lon_des \n" +
            ", ppp.inbound_ts \n" +
            "FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s ppp\n" +
            "  where pub_vol_idt = 0" +
            " and inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND PUB_IDT in (%s)";

    public static String GET_Promis_prmpricest_part = "SELECT\n" +
            "  ppp.pub_idt as PUB_IDT\n" +
            ", ppp.pub_vol_idt as PUB_VOL_IDT\n" +
            ", ppp.vol_prt_idt as VOL_PRT_IDT \n" +
            ", ppp.edn_idt as EDN_IDT \n" +
            ", ppp.prc_typ as PRC_TYP \n" +
            ", ppp.prc_geo as PRC_GEO \n" +
            ", ppp.ipt_cod as IPT_COD \n" +
            ", ppp.std_cur_cod as STD_CUR_COD \n" +
            ", cast(ppp.std_prc as double) as STD_PRC \n" +
            ", ppp.prc_pre as PRC_PRE\n" +
            ", ppp.add_prc as ADD_PRC\n" +
            "FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s ppp \n" +
            " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND PUB_IDT in (%s) order by pub_idt, std_prc, EDN_IDT, PRC_GEO";

    public static String GET_Promis_prmpubinft_part = "SELECT \n" +
            "  ppp.pub_idt \n" +
            ", ppp.sta_daa \n" +
            ", ppp.ful_tit \n" +
            ", ppp.pub_typ \n" +
            ", ppp.own_idt \n" +
            ", ppp.pbl_abr_nam \n" +
            ", ppp.loc \n" +
            ", ppp.sta_prm \n" +
            ", ppp.lng_cod \n" +
            ", ppp.pub_imp_idt \n" +
            ", ppp.pub_bgn_yea \n" +
            ", ppp.lco_num \n" +
            ", ppp.imp_dat \n" +
            ", ppp.cda \n" +
            ", ppp.cre_idt \n" +
            ", ppp.dep_idt \n" +
            ", ppp.lst_usr_idt \n" +
            ", ppp.lst_upd_dat \n" +
            ", ppp.abr_tit \n" +
            ", ppp.ful_tit_srt \n" +
            ", ppp.sub_tit_1 \n" +
            ", ppp.sub_tit_2 \n" +
            ", ppp.sub_tit_3 \n" +
            ", ppp.prg_sub_tit \n" +
            ", ppp.aut_edt_nam \n" +
            ", ppp.slo_txt \n" +
            ", ppp.ntb \n" +
            ", ppp.ftn \n" +
            ", ppp.for_tit \n" +
            ", ppp.rpn_inf \n" +
            ", ppp.adv_cod \n" +
            ", ppp.rvw_cod \n" +
            ", ppp.sup_app \n" +
            ", ppp.phy_sze \n" +
            ", ppp.fs_idt \n" +
            ", ppp.wtk_num \n" +
            ", ppp.wtk_cls \n" +
            ", ppp.bwk \n" +
            ", ppp.aei \n" +
            ", ppp.rev_edn_ti \n" +
            ", ppp.jnl_idt \n" +
            ", ppp.lst_upd_date \n" +
            ", ppp.cda_date \n" +
            ", ppp.aqs_cod \n" +
            ", ppp.mkt_cod \n" +
            ", ppp.aud \n" +
            ", ppp.sht_des \n" +
            ", ppp.sht_ctt_des \n" +
            ", ppp.slo_exp_dat \n" +
            ", ppp.if_no \n" +
            ", ppp.if_year \n" +
            ", ppp.if_comment \n" +
            ", ppp.aut_benefits \n" +
            ", ppp.source \n" +
            ", ppp.arg_cod \n" +
            ", ppp.if_5 \n" +
            ", ppp.if_ranking \n" +
            ", ppp.if_cat \n" +
            ", ppp.oa_type \n" +
            ", xw.epr as epr_id \n" +
            ", xw.work_type as work_type \n" +
            ", xp.epr as epr_product_id \n" +
            ", xp.product_type as product_type \n" +
            ", ppp.inbound_ts \n" +
            "FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s ppp \n" +
            "INNER JOIN " + GetDLCrossRefDB.getDLCrossRefDB() + " xw \n" +
            "  ON xw.identifier_type = 'ELSEVIER JOURNAL NUMBER' \n" +
            "  AND xw.record_level = 'Work' \n" +
            "  AND xw.identifier = ppp.jnl_idt \n" +
            "LEFT OUTER JOIN " + GetDLCrossRefDB.getDLCrossRefDB() + " xp \n" +
            "  ON xp.identifier_type = 'ELSEVIER JOURNAL NUMBER' \n" +
            "  AND xp.record_level = 'Product' \n" +
            "  AND xp.product_type = 'SUB' \n" +
            "  AND xp.manifestation_type = 'JPR' \n" +
            "  AND xp.identifier = ppp.jnl_idt \n" +
            "where source is null " +
            " and inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND PUB_IDT in (%s)";

    public static String GET_Promis_prmpubrelt_part = "select * from (SELECT\n" +
            "  ppp.pub_pub_idt\n" +
            ", ppp.rel_no\n" +
            ", ppp.rel_srt\n" +
            ", ppp.rel_idt\n" +
            ", ppp.rel_title\n" +
            ", ppp.footnote\n" +
            ", ppp.front_text\n" +
            ", ppp.end_text\n" +
            ", ppp.rtp_rtp_cod\n" +
            ", ppp.rel_end_date\n" +
            ", ppp.rel_start_date\n" +
            ", ppp.inbound_ts\n" +
            "FROM\n" + GetPRMDLDBUser.getPRMDataBase() + ".%s ppp\n" +
            "  WHERE (\n" +
            "    (rtp_rtp_cod = 'BOXST'\n" +
            "    and cast(ppp.pub_pub_idt as varchar)||ppp.rtp_rtp_cod||ppp.rel_title||cast(ppp.rel_no as varchar) IN\n" +
            "     (SELECT cast(pp1.pub_pub_idt as varchar)||pp1.rtp_rtp_cod||pp1.rel_title||cast(max(pp1.rel_no) as varchar)\n" +
            "      FROM " + GetPRMDLDBUser.getPRMDataBase() + ".%s pp1\n" +
            "      WHERE rtp_rtp_cod = 'BOXST'\n" +
            "      GROUP BY cast(pp1.pub_pub_idt as varchar)||pp1.rtp_rtp_cod||pp1.rel_title)\n" +
            "    )\n" +
            "    or (rtp_rtp_cod <> 'BOXST')\n" +
            "  ))where PUB_PUB_IDT in (%s)";

    public static String GET_promis_prmincpmct = "select\n" +
            "  pub_idt as PUB_IDT,\n" +
            "  mkt_idt as MKT_IDT,\n" +
            "  mkt_sub_idt as MKT_SUB_IDT,\n" +
            "  inbound_ts\n" +
            "FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s" +
            " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND PUB_IDT in (%s)";

    public static String GET_Promis_prmpmccodt = "select\n" +
            "  mkt_idt,\n" +
            "  mkt_des,\n" +
            "  div_idt,\n" +
            "  inbound_ts\n" +
            "FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s" +
            " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND MKT_IDT in ('%s')";

    public static String GETPRMPUBT ="select \n" +
                "PUB_IDT as PUB_IDT\n" +
                ",AUT_EDT_IDT as AUT_EDT_IDT\n" +
                ",AUT_EDT_TYP as AUT_EDT_TYP\n" +
                ",TYP_DES as TYP_DES\n" +
                ",SEQ_NUM as SEQ_NUM\n" +
                ",AUT_EDT_PRE as AUT_EDT_PRE\n" +
                ",AUT_EDT_INI as AUT_EDT_INI\n" +
                ",AUT_EDT_NAM as AUT_EDT_NAM\n" +
                ",AUT_EDT_SORT as AUT_EDT_SORT\n" +
                ",AUT_EDT_SUF as AUT_EDT_SUF\n" +
                ",AFF_TXT as AFF_TXT\n" +
                ",FTN as FTN\n" +
                ",AUT_EDT_JCI as AUT_EDT_JCI\n" +
                ",BIO_IMAGE as BIO_IMAGE\n" +
                "from " + GetPRMDLDBUser.getPRMDataBase() + ".%s\n" +
                " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
                " AND PUB_IDT in (%s) order by AUT_EDT_IDT";

    public static String GETPRMCLSCODT ="select \n" +
            "CLS_COD as CLS_COD\n" +
            ",CLS_DES as CLS_DES\n" +
            "from " + GetPRMDLDBUser.getPRMDataBase() + ".%s\n" +
            " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND CLS_COD in ('%s')";

    public static String GETPRMCLST = "select \n" +
            "PUB_IDT as PUB_IDT\n" +
            ",CLS_COD as CLS_COD\n" +
            "from " + GetPRMDLDBUser.getPRMDataBase() + ".%s\n" +
            " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND PUB_IDT in (%s) order by CLS_COD";

    public static String GETPRMLONDEST = "select \n" +
            "PUB_IDT as PUB_IDT\n" +
            ",PUB_VOL_IDT as PUB_VOL_IDT\n" +
            ",VOL_PRT_IDT as VOL_PRT_IDT\n" +
             ",LON_DES as LON_DES\n" +
            "from " + GetPRMDLDBUser.getPRMDataBase() + ".%s\n" +
            " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND PUB_IDT in (%s)";

    public static String GETPRMPRICEST = "select \n" +
            "PUB_IDT as PUB_IDT\n" +
            ",PUB_VOL_IDT as PUB_VOL_IDT\n" +
            ",VOL_PRT_IDT as VOL_PRT_IDT\n" +
            ",EDN_IDT as EDN_IDT\n" +
            ",PRC_TYP as PRC_TYP\n" +
            ",PRC_GEO as PRC_GEO\n" +
            ",IPT_COD as IPT_COD\n" +
            ",STD_CUR_COD as STD_CUR_COD\n" +
            ",STD_PRC as STD_PRC\n" +
            ",PRC_PRE as PRC_PRE\n" +
            ",ADD_PRC as ADD_PRC\n" +
            "from " + GetPRMDLDBUser.getPRMDataBase() + ".%s\n" +
            " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND PUB_IDT in (%s) order by pub_idt, std_prc, EDN_IDT, PRC_GEO";

    public static String GETPRMPUBINFT = "select \n" +
            "PUB_IDT as PUB_IDT\n" +
            ",STA_DAA as STA_DAA\n" +
            ",FUL_TIT as FUL_TIT\n" +
            ",PUB_TYP as PUB_TYP\n" +
            ",OWN_IDT as OWN_IDT\n" +
            ",PBL_ABR_NAM as PBL_ABR_NAM\n" +
            ",LOC as LOC\n" +
            ",STA_PRM as STA_PRM\n" +
            ",LNG_COD as LNG_COD\n" +
            ",PUB_IMP_IDT as PUB_IMP_IDT\n" +
            ",PUB_BGN_YEA as PUB_BGN_YEA\n" +
            ",LCO_NUM as LCO_NUM\n" +
            ",IMP_DAT as IMP_DAT\n" +
            ",CDA as CDA\n" +
            ",CRE_IDT as CRE_IDT\n" +
            ",DEP_IDT as DEP_IDT\n" +
            ",LST_USR_IDT as LST_USR_IDT\n" +
            ",LST_UPD_DAT as LST_UPD_DAT\n" +
            ",ABR_TIT as ABR_TIT\n" +
            ",FUL_TIT_SRT as FUL_TIT_SRT\n" +
            ",SUB_TIT_1 as SUB_TIT_1\n" +
            ",SUB_TIT_2 as SUB_TIT_2\n" +
            ",SUB_TIT_3 as SUB_TIT_3\n" +
            ",PRG_SUB_TIT as PRG_SUB_TIT\n" +
            ",AUT_EDT_NAM as AUT_EDT_NAM\n" +
            ",SLO_TXT as SLO_TXT\n" +
            ",NTB as NTB\n" +
            ",FTN as FTN\n" +
            ",FOR_TIT as FOR_TIT\n" +
            ",RPN_INF as RPN_INF\n" +
            ",ADV_COD as ADV_COD\n" +
            ",RVW_COD as RVW_COD\n" +
            ",SUP_APP as SUP_APP\n" +
            ",PHY_SZE as PHY_SZE\n" +
            ",FS_IDT as FS_IDT\n" +
            ",WTK_NUM as WTK_NUM\n" +
            ",WTK_CLS as WTK_CLS\n" +
            ",BWK as BWK\n" +
            ",AEI as AEI\n" +
            ",REV_EDN_TI as REV_EDN_TI\n" +
            ",JNL_IDT as JNL_IDT\n" +
            ",LST_UPD_DATE as LST_UPD_DATE\n" +
            ",CDA_DATE as CDA_DATE\n" +
            ",AQS_COD as AQS_COD\n" +
            ",MKT_COD as MKT_COD\n" +
            ",AUD as AUD\n" +
            ",SHT_DES as SHT_DES\n" +
            ",SHT_CTT_DES as SHT_CTT_DES\n" +
            ",SLO_EXP_DAT as SLO_EXP_DAT\n" +
            ",IF_NO as IF_NO\n" +
            ",IF_YEAR as IF_YEAR\n" +
            ",IF_COMMENT as IF_COMMENT\n" +
            ",AUT_BENEFITS as AUT_BENEFITS\n" +
            ",SOURCE as SOURCE\n" +
            ",ARG_COD as ARG_COD\n" +
            ",IF_5 as IF_5\n" +
            ",IF_RANKING as IF_RANKING\n" +
            ",IF_CAT as IF_CAT\n" +
            ",OA_TYPE as OA_TYPE\n" +
            "from " + GetPRMDLDBUser.getPRMDataBase() + ".%s\n" +
            " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND PUB_IDT in (%s)";

    public static String GETPRMPUBRELT = "select \n" +
            "PUB_PUB_IDT as PUB_PUB_IDT\n" +
            ",REL_NO as REL_NO\n" +
            ",REL_SRT as REL_SRT\n" +
            ",REL_IDT as REL_IDT\n" +
            ",REL_TITLE as REL_TITLE\n" +
            ",FOOTNOTE as FOOTNOTE\n" +
            ",FRONT_TEXT as FRONT_TEXT\n" +
            ",END_TEXT as END_TEXT\n" +
            ",RTP_RTP_COD as RTP_RTP_COD\n" +
            ",REL_END_DATE as REL_END_DATE\n" +
            ",REL_START_DATE as REL_START_DATE\n" +
            "from " + GetPRMDLDBUser.getPRMDataBase() + ".%s\n" +
            " where inbound_ts = (select inbound_ts from " + GetPRMDLDBUser.getPRMDataBase() + ".%s order by inbound_ts desc limit 1)\n" +
            " AND PUB_PUB_IDT in (%s)";

    public static String GET_SUBJECT_AREAS = "select pub_idt as PUB_IDT,\n"
    + "epr_id as EPR_ID,\n"
    + "u_key as U_KEY,\n"
    + "priority as PRIORITY,\n"
    + "subject_area_code as SUBJECT_AREA_CODE,\n"
    + "subject_area_name as SUBJECT_AREA_NAME,\n"
    + "subject_type_code as SUBJECT_TYPE_CODE,\n"
    + "subject_type_name as SUBJECT_TYPE_NAME,\n"
    + "work_type as WORK_TYPE from " + GetPRMDLDBUser.getPRMDataBase() + ".%s where U_KEY in ('%s') order by pub_idt";

    public static String GET_PRICING = "select pub_idt as PUB_IDT,\n" +
            "epr_id as EPR_ID,\n" +
            "product_type as PRODUCT_TYPE,\n" +
            "u_key as U_KEY,\n" +
            "currency as CURRENCY,\n" +
            "price as PRICE,\n" +
            "start_date as START_DATE,\n" +
            "end_date as END_DATE,\n" +
            "region as REGION,\n" +
            "quantity as QUANTITY,\n" +
            "customer_category as CUSTOMER_CATEGORY from "+ GetPRMDLDBUser.getPRMDataBase() + ".%s where U_KEY in ('%s')";

    public static String GET_PERSON_ROLES = "select * from (select pub_idt as PUB_IDT,\n" +
            "epr_id as EPR_ID,\n" +
            "u_key as U_KEY,\n" +
            "role_description as ROLE_DESCRIPTION,\n" +
            "sequence_number as SEQUENCE_NUMBER,\n" +
            "group_number as GROUP_NUMBER,\n" +
            "initials as INITIALS,\n" +
            "last_name as LAST_NAME,\n" +
            "title as TITLE,\n" +
            "honours as HONOURS,\n" +
            "affiliation as AFFILIATION,\n" +
            "image_url as IMAGE_URL,\n" +
            "footnote_txt as FOOTNOTE_TXT,\n" +
            "notes_txt as NOTES_TXT,\n" +
            "work_type as WORK_TYPE from "+ GetPRMDLDBUser.getPRMDataBase() + ".%s where U_KEY in ('%s')) order by pub_idt desc, sequence_number desc, initials desc, affiliation desc";

    public static String GET_WORKS = "select pub_idt as PUB_IDT,\n" +
            "epr_id as EPR_ID,\n" +
            "u_key as U_KEY,\n" +
            "journal_aims_scope as JOURNAL_AIMS_SCOPE,\n" +
            "elsevier_com_ind as ELSEVIER_COM_IND,\n" +
            "primary_author as PRIMARY_AUTHOR,\n" +
            "previous_title as PREVIOUS_TITLE,\n" +
            "internal_elsevier_division,\n" +
            "work_type as WORK_TYPE from "+ GetPRMDLDBUser.getPRMDataBase() + ".%s where U_KEY in ('%s') order by u_key desc";

    public static String GET_METRICS = "select pub_idt as PUB_IDT,\n" +
            "epr_id as EPR_ID,\n" +
            "u_key as U_KEY,\n" +
            "metric_code as METRIC_CODE,\n" +
            "metric_name as METRIC_NAME,\n" +
            "metric as METRIC,\n" +
            "metric_year as METRIC_YEAR,\n" +
            "metric_url as METRIC_URL,\n" +
            "work_type as WORK_TYPE from "+ GetPRMDLDBUser.getPRMDataBase() + ".%s where U_KEY in ('%s')";

    public static String GET_URLS = "select pub_idt as PUB_IDT," +
            "epr_id as EPR_ID,\n" +
            "u_key as U_KEY,\n" +
            "url_code as URL_CODE,\n" +
            "url_name as URL_NAME,\n" +
            "url as URL,\n" +
            "url_title as URL_TITLE,\n" +
            "work_type as WORK_TYPE from "+ GetPRMDLDBUser.getPRMDataBase() + ".%s where U_KEY in ('%s')";

    public static String GET_WORK_RELS = "select parent_pub_idt as PARENT_PUB_IDT," +
            "parent_epr_id as PARENT_EPR_ID,\n" +
            "u_key as U_KEY,\n" +
            "child_pub_idt as CHILD_PUB_IDT,\n" +
            "child_epr_id as CHILD_EPR_ID,\n" +
            "child_title as CHILD_TITLE,\n" +
            "child_related_type_code as CHILD_RELATED_TYPE_CODE,\n" +
            "child_related_type_name as CHILD_RELATED,\n" +
            "child_related_type_roll_up as CHILD_RELATED_TYPE_ROLL_UP,\n" +
            "child_related_status_code as CHILD_RELATED_STATUS_CODE,\n" +
            "child_related_status_name as CHILD_RELATED_STATUS_NAME,\n" +
            "child_related_status_roll_up as CHILD_RELATED_STATUS_ROLL_UP,\n" +
            "relationship_type_code as RELATIONSHIP_TYPE_CODE,\n" +
            "relationship_type_name as RELATIONSHIP_TYPE_NAME,\n" +
            "work_type as WORK_TYPE from "+ GetPRMDLDBUser.getPRMDataBase() + ".%s where U_KEY in ('%s')";

    public static String GET_SUBJECT_AREAS_DeltaQUERY = "select * from (with crr_dataset as( \n" +
            "  select pub_idt,epr_id,u_key,priority,subject_area_code,subject_area_name,subject_type_code,subject_type_name,work_type,transform_file_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select pub_idt as prev_pub_idt,epr_id as prev_epr_id,u_key as prev_u_key,priority as prev_priority,subject_area_code as prev_subject_area_code,subject_area_name as prev_subject_area_name,subject_type_code as prev_subject_type_code,subject_type_name as prev_subject_type_name,work_type as prev_work_type,transform_file_ts\n" +
            "  from "+GetPRMDLDBUser.getPRMDataBase()+".%s\n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".%s dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ") \n" +
            "select 'new', * \n" +
            "from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where prev.prev_u_key is null\n" +
            "    union\n" +
            "select 'deleted', * \n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.prev_u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', * \n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where (coalesce(crr.pub_idt, null) <> coalesce(prev.prev_pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.prev_u_key, 'null') or\n" +
            "coalesce(crr.epr_id, 'null') <> coalesce(prev.prev_epr_id, 'null') or\n" +
            "coalesce(crr.priority, null) <> coalesce(prev.prev_priority, null) or\n" +
            "coalesce(crr.subject_area_code, 'null') <> coalesce(prev.prev_subject_area_code, 'null') or\n" +
            "coalesce(crr.subject_area_name, 'null') <> coalesce(prev.prev_subject_area_name, 'null') or\n" +
            "coalesce(crr.subject_type_code, 'null') <> coalesce(prev.prev_subject_type_code, 'null') or\n" +
            "coalesce(crr.subject_type_name, 'null') <> coalesce(prev.prev_subject_type_name, 'null') or\n" +
            "coalesce(crr.work_type, 'null') <> coalesce(prev.prev_work_type, 'null'))) where u_key in ('%s')";

    public static String GET_PRICING_DeltaQUERY = "select * from (with crr_dataset as( \n" +
            "  select pub_idt,epr_id,product_type,u_key,currency,price,start_date,end_date,region,quantity,customer_category,transform_file_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select pub_idt as prev_pub_idt,epr_id as prev_epr_id,product_type as prev_product_type,u_key as prev_u_key,currency as prev_currency,price as prev_price,start_date as prev_start_date,end_date as prev_end_date,region as prev_region,quantity as prev_quantity,customer_category as prev_customer_category,transform_file_ts\n" +
            "  from "+GetPRMDLDBUser.getPRMDataBase()+".%s\n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".%s dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ") \n" +
            "select 'new', *\n" +
            "from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where prev.prev_u_key is null\n" +
            "    union\n" +
            "select 'deleted', *\n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.prev_u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', *\n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where (coalesce(crr.pub_idt, null) <> coalesce(prev.prev_pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.prev_u_key, 'null') or\n" +
            "coalesce(crr.epr_id, 'null') <> coalesce(prev.prev_epr_id, 'null') or\n" +
            "coalesce(crr.product_type, 'null') <> coalesce(prev.prev_product_type, 'null') or\n" +
            "coalesce(crr.currency, 'null') <> coalesce(prev.prev_currency, 'null') or\n" +
            "coalesce(crr.price, null) <> coalesce(prev.prev_price, null) or\n" +
            "coalesce(crr.start_date, null) <> coalesce(prev.prev_start_date, null) or\n" +
            "coalesce(crr.end_date, null) <> coalesce(prev.prev_end_date, null) or\n" +
            "coalesce(crr.region, 'null') <> coalesce(prev.prev_region, 'null') or\n" +
            "coalesce(crr.quantity, null) <> coalesce(prev.prev_quantity, null) or\n" +
            "coalesce(crr.customer_category, 'null') <> coalesce(prev.prev_customer_category, 'null'))) where u_key in ('%s')";

    public static String GET_PERSON_ROLES_DeltaQUERY = "select * from (with crr_dataset as( \n" +
            "  select pub_idt,epr_id,u_key,role_description,sequence_number,group_number,initials,last_name,title,honours,affiliation,image_url,footnote_txt,notes_txt,work_type,transform_file_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select pub_idt as prev_pub_idt,epr_id as prev_epr_id,u_key as prev_u_key,role_description as prev_role_description,sequence_number as prev_sequence_number,group_number as prev_group_number,initials as prev_initials,last_name as prev_last_name,title as prev_title,honours as prev_honours,affiliation as prev_affiliation,image_url as prev_image_url,footnote_txt as prev_footnote_txt,notes_txt as prev_notes_txt,work_type as prev_work_type,transform_file_ts\n" +
            "  from "+GetPRMDLDBUser.getPRMDataBase()+".%s\n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".%s dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ") \n" +
            "select 'new', *\n" +
            "from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where prev.prev_u_key is null\n" +
            "    union\n" +
            "select 'deleted', *\n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.prev_u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', *\n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where (coalesce(crr.pub_idt, null) <> coalesce(prev.prev_pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.prev_u_key, 'null') or\n" +
            "coalesce(crr.epr_id, 'null') <> coalesce(prev.prev_epr_id, 'null') or\n" +
            "coalesce(crr.role_description, 'null') <> coalesce(prev.prev_role_description, 'null') or\n" +
            "coalesce(crr.sequence_number, null) <> coalesce(prev.prev_sequence_number, null) or\n" +
            "coalesce(crr.group_number, null) <> coalesce(prev.prev_group_number, null) or\n" +
            "coalesce(crr.initials, 'null') <> coalesce(prev.prev_initials, 'null') or\n" +
            "coalesce(crr.last_name, 'null') <> coalesce(prev.prev_last_name, 'null') or\n" +
            "coalesce(crr.title, 'null') <> coalesce(prev.prev_title, 'null') or\n" +
            "coalesce(crr.honours, 'null') <> coalesce(prev.prev_honours, 'null') or\n" +
            "coalesce(crr.affiliation, 'null') <> coalesce(prev.prev_affiliation, 'null') or \n" +
            "coalesce(crr.image_url, 'null') <> coalesce(prev.prev_image_url, 'null') or \n" +
            "coalesce(crr.footnote_txt, 'null') <> coalesce(prev.prev_footnote_txt, 'null') or \n" +
            "coalesce(crr.notes_txt, 'null') <> coalesce(prev.prev_notes_txt, 'null') or \n" +
            "coalesce(crr.work_type, 'null') <> coalesce(prev.prev_work_type, 'null'))) where u_key in ('%s')";

    public static String GET_WORKS_DeltaQUERY = "select * from (with crr_dataset as( \n" +
            "  select pub_idt,epr_id,u_key,journal_aims_scope,elsevier_com_ind,primary_author,previous_title,internal_elsevier_division,work_type,transform_file_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select pub_idt as prev_pub_idt,epr_id as prev_epr_id,u_key as prev_u_key,journal_aims_scope as prev_journal_aims_scope,elsevier_com_ind as prev_elsevier_com_ind,primary_author as prev_primary_author,previous_title as prev_previous_title,internal_elsevier_division as prev_internal_elsevier_division,work_type as prev_work_type,transform_file_ts\n" +
            "  from "+GetPRMDLDBUser.getPRMDataBase()+".%s\n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".%s dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ") \n" +
            "select 'new', *\n" +
            "from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where prev.prev_u_key is null\n" +
            "    union\n" +
            "select 'deleted', *\n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.prev_u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', *\n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where (coalesce(crr.pub_idt, null) <> coalesce(prev.prev_pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.prev_u_key, 'null') or\n" +
            "coalesce(crr.epr_id, 'null') <> coalesce(prev.prev_epr_id, 'null') or\n" +
            "coalesce(crr.journal_aims_scope, 'null') <> coalesce(prev.prev_journal_aims_scope, 'null') or\n" +
            "coalesce(crr.elsevier_com_ind, null) <> coalesce(prev.prev_elsevier_com_ind, null) or\n" +
            "coalesce(crr.primary_author, null) <> coalesce(prev.prev_primary_author, null) or\n" +
            "coalesce(crr.previous_title, 'null') <> coalesce(prev.prev_previous_title, 'null') or\n" +
            "coalesce(crr.internal_elsevier_division, 'null') <> coalesce(prev.prev_internal_elsevier_division, 'null') or\n" +
            "coalesce(crr.work_type, 'null') <> coalesce(prev.prev_work_type, 'null'))) where u_key in ('%s')";

    public static String GET_METRICS_DeltaQUERY = "select * from (with crr_dataset as( \n" +
            "  select pub_idt,epr_id,u_key,metric_code,metric_name,metric,metric_year,metric_url,work_type,transform_file_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select pub_idt as prev_pub_idt,epr_id as prev_epr_id,u_key as prev_u_key,metric_code as prev_metric_code,metric_name as prev_metric_name,metric as prev_metric,metric_year as prev_metric_year,metric_url as prev_metric_url,work_type as prev_work_type,transform_file_ts\n" +
            "  from "+GetPRMDLDBUser.getPRMDataBase()+".%s\n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".%s dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ") \n" +
            "select 'new', *\n" +
            "from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where prev.prev_u_key is null\n" +
            "    union\n" +
            "select 'deleted', *\n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.prev_u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', *\n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where (coalesce(crr.pub_idt, null) <> coalesce(prev.prev_pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.prev_u_key, 'null') or\n" +
            "coalesce(crr.epr_id, 'null') <> coalesce(prev.prev_epr_id, 'null') or\n" +
            "coalesce(crr.metric_code, 'null') <> coalesce(prev.prev_metric_code, 'null') or\n" +
            "coalesce(crr.metric_name, 'null') <> coalesce(prev.prev_metric_name, 'null') or\n" +
            "coalesce(crr.metric, null) <> coalesce(prev.prev_metric, null) or\n" +
            "coalesce(crr.metric_year, null) <> coalesce(prev.prev_metric_year, null) or\n" +
            "coalesce(crr.metric_url, 'null') <> coalesce(prev.prev_metric_url, 'null') or\n" +
            "coalesce(crr.work_type, 'null') <> coalesce(prev.prev_work_type, 'null'))) where u_key in ('%s')";

    public static String GET_URLS_DeltaQUERY = "select * from (with crr_dataset as( \n" +
            "  select pub_idt,epr_id,u_key,url_code,url_name,url,url_title,work_type,transform_file_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select pub_idt as prev_pub_idt,epr_id as prev_epr_id,u_key as prev_u_key,url_code as prev_url_code,url_name as prev_url_name,url as prev_url,url_title as prev_url_title,work_type as prev_work_type,transform_file_ts\n" +
            "  from "+GetPRMDLDBUser.getPRMDataBase()+".%s\n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".%s dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ") \n" +
            "select 'new', *\n" +
            "from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where prev.prev_u_key is null\n" +
            "    union\n" +
            "select 'deleted', *\n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.prev_u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', *\n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where (coalesce(crr.pub_idt, null) <> coalesce(prev.prev_pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.prev_u_key, 'null') or\n" +
            "coalesce(crr.epr_id, 'null') <> coalesce(prev.prev_epr_id, 'null') or\n" +
            "coalesce(crr.url_code, 'null') <> coalesce(prev.prev_url_code, 'null') or\n" +
            "coalesce(crr.url_name, 'null') <> coalesce(prev.prev_url_name, 'null') or\n" +
            "coalesce(crr.url, 'null') <> coalesce(prev.prev_url, 'null') or\n" +
            "coalesce(crr.url_title, 'null') <> coalesce(prev.prev_url_title, 'null') or\n" +
            "coalesce(crr.work_type, 'null') <> coalesce(prev.prev_work_type, 'null'))) where u_key in ('%s')";

    public static String GET_WORK_RELS_DeltaQUERY = "select * from (with crr_dataset as( \n" +
            "  select parent_pub_idt,parent_epr_id,u_key,child_pub_idt,child_epr_id,child_title,child_related_type_code,child_related_type_name,child_related_type_roll_up,child_related_status_code,child_related_status_name,child_related_status_roll_up,relationship_type_code,relationship_type_name,work_type,transform_file_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select parent_pub_idt as prev_parent_pub_idt,parent_epr_id as prev_parent_epr_id,u_key as prev_u_key,child_pub_idt as prev_child_pub_idt,child_epr_id as prev_child_epr_id,child_title as prev_child_title,child_related_type_code as prev_child_related_type_code,child_related_type_name as prev_child_related_type_name,child_related_type_roll_up as prev_child_related_type_roll_up,child_related_status_code as prev_child_related_status_code,child_related_status_name as prev_child_related_status_name,child_related_status_roll_up as prev_child_related_status_roll_up,relationship_type_code as prev_relationship_type_code,relationship_type_name as prev_relationship_type_name,work_type as prev_work_type,transform_file_ts\n" +
            "  from "+GetPRMDLDBUser.getPRMDataBase()+".%s\n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".%s dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ") \n" +
            "select 'new', *\n" +
            "from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where prev.prev_u_key is null\n" +
            "    union\n" +
            "select 'deleted', *\n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.prev_u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', *\n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.prev_u_key\n" +
            "where (coalesce(crr.parent_pub_idt, null) <> coalesce(prev.prev_parent_pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.prev_u_key, 'null') or\n" +
            "coalesce(crr.parent_epr_id, 'null') <> coalesce(prev.prev_parent_epr_id, 'null') or\n" +
            "coalesce(crr.child_pub_idt, null) <> coalesce(prev.prev_child_pub_idt, null) or\n" +
            "coalesce(crr.child_epr_id, 'null') <> coalesce(prev.prev_child_epr_id, 'null') or\n" +
            "coalesce(crr.child_title, 'null') <> coalesce(prev.prev_child_title, 'null') or\n" +
            "coalesce(crr.child_related_type_code, 'null') <> coalesce(prev.prev_child_related_type_code, 'null') or\n" +
            "coalesce(crr.child_related_type_roll_up, 'null') <> coalesce(prev.prev_child_related_type_roll_up, 'null') or\n" +
            "coalesce(crr.child_related_status_code, 'null') <> coalesce(prev.prev_child_related_status_code, 'null') or\n" +
            "coalesce(crr.child_related_status_name, 'null') <> coalesce(prev.prev_child_related_status_name, 'null') or\n" +
            "coalesce(crr.child_related_status_roll_up, 'null') <> coalesce(prev.prev_child_related_status_roll_up, 'null') or\n" +
            "coalesce(crr.relationship_type_code, 'null') <> coalesce(prev.prev_relationship_type_code, 'null') or\n" +
            "coalesce(crr.relationship_type_name, 'null') <> coalesce(prev.prev_relationship_type_name, 'null') or\n" +
            "coalesce(crr.work_type, 'null') <> coalesce(prev.prev_work_type, 'null') or\n" +
            "coalesce(crr.child_related_type_name, 'null') <> coalesce(prev.prev_child_related_type_name, 'null'))) where u_key in ('%s')";

    public static String GET_DELTA = "select * from "+GetPRMDLDBUser.getPRMDataBase()+".%s where u_key in ('%s')";

    public static String GET_SUBJECT_AREAS_HistExclQUERY = "select\n" +
            "  pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", priority\n" +
            ", subject_area_code\n" +
            ", subject_area_name\n" +
            ", subject_type_code\n" +
            ", subject_type_name\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", transform_ts\n" +
            ", last_updated_date\n" +
            ", delete_flag\n" +
            " from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_subject_areas_part tdh\n" +
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_subject_areas) and U_key in ('%s')";

    public static String GET_PRICING_HistExclQUERY = "select\n" +
            "  pub_idt\n" +
            ", epr_id\n" +
            ", product_type\n" +
            ", u_key\n" +
            ", currency\n" +
            ", price\n" +
            ", start_date\n" +
            ", end_date\n" +
            ", region\n" +
            ", quantity\n" +
            ", customer_category\n" +
            ", inbound_ts\n" +
            ", transform_ts\n" +
            ", last_updated_date\n" +
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_pricing_part tdh\n" +
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_pricing) and U_key in ('%s')";

    public static String GET_PERSON_ROLES_HistExclQUERY = "select * from (select\n" +
            "  pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", role_description\n" +
            ", sequence_number\n" +
            ", group_number\n" +
            ", initials\n" +
            ", last_name\n" +
            ", title\n" +
            ", honours\n" +
            ", affiliation\n" +
            ", image_url\n" +
            ", footnote_txt\n" +
            ", notes_txt\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", transform_ts\n" +
            ", last_updated_date\n" +
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_person_roles_part tdh\n" +
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_person_roles) and U_key in ('%s')) order by pub_idt desc, sequence_number desc, initials desc , affiliation desc";

    public static String GET_WORKS_HistExclQUERY = "select\n" +
            "  pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", journal_aims_scope\n" +
            ", elsevier_com_ind\n" +
            ", primary_author\n" +
            ", previous_title\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", transform_ts\n" +
            ", last_updated_date\n" +
            ",internal_elsevier_division\n" +
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_works_part tdh\n" +
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_works) and U_key in ('%s') order by u_key desc";

    public static String GET_METRICS_HistExclQUERY = "select\n" +
            "  pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", metric_code\n" +
            ", metric_name\n" +
            ", metric\n" +
            ", metric_year\n" +
            ", metric_url\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", transform_ts\n" +
            ", last_updated_date\n" +
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_metrics_part tdh\n" +
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_metrics) and U_key in ('%s')";

    public static String GET_URLS_HistExclQUERY = "select\n" +
            "  pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", url_code\n" +
            ", url_name\n" +
            ", url\n" +
            ", url_title\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", transform_ts\n" +
            ", last_updated_date\n" +
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_urls_part tdh\n" +
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_urls) and U_key in ('%s')";

    public static String GET_WORK_RELS_HistExclQUERY = "select\n" +
            "  parent_pub_idt\n" +
            ", parent_epr_id\n" +
            ", u_key\n" +
            ", child_pub_idt\n" +
            ", child_epr_id\n" +
            ", child_title\n" +
            ", child_related_type_code\n" +
            ", child_related_type_name\n" +
            ", child_related_type_roll_up\n" +
            ", child_related_status_code\n" +
            ", child_related_status_name\n" +
            ", child_related_status_roll_up\n" +
            ", relationship_type_code\n" +
            ", relationship_type_name\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", transform_ts\n" +
            ", last_updated_date\n" +
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_work_rels_part tdh\n" +
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_work_rels) and U_key in ('%s')";

    public static String GET_SUBJECT_AREAS_LatestQUERY="select * from (select pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", priority\n" +
            ", subject_area_code\n" +
            ", subject_area_name\n" +
            ", subject_type_code\n" +
            ", subject_type_name\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", try(date_parse(insert_timestamp,'%%Y-%%m-%%dT%%H:%%i:%%s.%%fZ'))  as last_updated_date\n" +
            ", case when delta_mode = 'D' then true else false end as delete_flag \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_subject_areas\n" +
            "union all\n" +
            "select pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", priority\n" +
            ", subject_area_code\n" +
            ", subject_area_name\n" +
            ", subject_type_code\n" +
            ", subject_type_name\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", last_updated_date\n" +
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_subject_areas_excl_delta) where U_key in ('%s') order by PUB_IDT";

    public static String GET_PRICING_LatestQUERY="select * from (select pub_idt\n" +
            ", epr_id\n" +
            ", product_type\n" +
            ", u_key\n" +
            ", currency\n" +
            ", price\n" +
            ", start_date\n" +
            ", end_date\n" +
            ", region\n" +
            ", quantity\n" +
            ", customer_category\n" +
            ", inbound_ts\n" +
            ", try(date_parse(insert_timestamp,'%%Y-%%m-%%dT%%H:%%i:%%s.%%fZ'))  as last_updated_date\n" +
            ", case when delta_mode = 'D' then true else false end as delete_flag \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_pricing\n" +
            "union all\n" +
            "select pub_idt\n" +
            ", epr_id\n" +
            ", product_type\n" +
            ", u_key\n" +
            ", currency\n" +
            ", price\n" +
            ", start_date\n" +
            ", end_date\n" +
            ", region\n" +
            ", quantity\n" +
            ", customer_category\n" +
            ", inbound_ts\n" +
            ", last_updated_date\n" +
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_pricing_excl_delta) where U_key in ('%s')";

    public static String GET_PERSON_ROLES_LatestQUERY="select * from (select pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", role_description\n" +
            ", sequence_number\n" +
            ", group_number\n" +
            ", initials\n" +
            ", last_name\n" +
            ", title\n" +
            ", honours\n" +
            ", affiliation\n" +
            ", image_url\n" +
            ", footnote_txt\n" +
            ", notes_txt\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", try(date_parse(insert_timestamp,'%%Y-%%m-%%dT%%H:%%i:%%s.%%fZ'))  as last_updated_date\n" +
            ", case when delta_mode = 'D' then true else false end as delete_flag \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_person_roles\n" +
            "union all\n" +
            "select pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", role_description\n" +
            ", sequence_number\n" +
            ", group_number\n" +
            ", initials\n" +
            ", last_name\n" +
            ", title\n" +
            ", honours\n" +
            ", affiliation\n" +
            ", image_url\n" +
            ", footnote_txt\n" +
            ", notes_txt\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", last_updated_date\n" +
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_person_roles_excl_delta) where U_key in ('%s')";

    public static String GET_WORKS_LatestQUERY="select * from (select pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", journal_aims_scope\n" +
            ", elsevier_com_ind\n" +
            ", primary_author\n" +
            ", previous_title\n" +
            ", work_type\n" +
            ", internal_elsevier_division\n" +
            ", inbound_ts\n" +
            ", try(date_parse(insert_timestamp,'%%Y-%%m-%%dT%%H:%%i:%%s.%%fZ'))  as last_updated_date\n" +
            ", case when delta_mode = 'D' then true else false end as delete_flag \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_works\n" +
            "union all\n" +
            "select pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", journal_aims_scope\n" +
            ", elsevier_com_ind\n" +
            ", primary_author\n" +
            ", previous_title\n" +
            ", work_type\n" +
            ", internal_elsevier_division\n" +
            ", inbound_ts\n" +
            ", last_updated_date\n" +
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_works_excl_delta) where U_KEY in ('%s')";

    public static String GET_METRICS_LatestQUERY="select * from (select pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", metric_code\n" +
            ", metric_name\n" +
            ", metric\n" +
            ", metric_year\n" +
            ", metric_url\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", try(date_parse(insert_timestamp,'%%Y-%%m-%%dT%%H:%%i:%%s.%%fZ'))  as last_updated_date\n" +
            ", case when delta_mode = 'D' then true else false end as delete_flag \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_metrics\n" +
            "union all\n" +
            "select pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", metric_code\n" +
            ", metric_name\n" +
            ", metric\n" +
            ", metric_year\n" +
            ", metric_url\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", last_updated_date, delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_metrics_excl_delta) where U_key in ('%s')";

    public static String GET_URLS_LatestQUERY="select * from (select pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", url_code\n" +
            ", url_name\n" +
            ", url\n" +
            ", url_title\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", try(date_parse(insert_timestamp,'%%Y-%%m-%%dT%%H:%%i:%%s.%%fZ'))  as last_updated_date\n" +
            ", case when delta_mode = 'D' then true else false end as delete_flag \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_urls\n" +
            "union all\n" +
            "select  pub_idt\n" +
            ", epr_id\n" +
            ", u_key\n" +
            ", url_code\n" +
            ", url_name\n" +
            ", url\n" +
            ", url_title\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", last_updated_date\n" +
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_urls_excl_delta) where U_key in ('%s')";

    public static String GET_WORK_RELS_LatestQUERY="select * from (select parent_pub_idt\n" +
            ", parent_epr_id\n" +
            ", u_key\n" +
            ", child_pub_idt\n" +
            ", child_epr_id\n" +
            ", child_title\n" +
            ", child_related_type_code\n" +
            ", child_related_type_name\n" +
            ", child_related_type_roll_up\n" +
            ", child_related_status_code\n" +
            ", child_related_status_name\n" +
            ", child_related_status_roll_up\n" +
            ", relationship_type_code\n" +
            ", relationship_type_name\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", try(date_parse(insert_timestamp,'%%Y-%%m-%%dT%%H:%%i:%%s.%%fZ'))  as last_updated_date\n" +
            ", case when delta_mode = 'D' then true else false end as delete_flag \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_work_rels\n" +
            "union all\n" +
            "select parent_pub_idt\n" +
            ", parent_epr_id\n" +
            ", u_key\n" +
            ", child_pub_idt\n" +
            ", child_epr_id\n" +
            ", child_title\n" +
            ", child_related_type_code\n" +
            ", child_related_type_name\n" +
            ", child_related_type_roll_up\n" +
            ", child_related_status_code\n" +
            ", child_related_status_name\n" +
            ", child_related_status_roll_up\n" +
            ", relationship_type_code\n" +
            ", relationship_type_name\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            ", last_updated_date\n" +
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_work_rels_excl_delta) where U_key in ('%s')";

    public static String GET_SUBJECT_AREAS_TransformMapping="select * from (select cls.pub_idt\n" +
            ", inf.epr_id\n" +
            ", cast(cls.pub_idt as varchar)||cod.cls_cod as u_key\n" +
            ", cls.priority\n" +
            ", cod.cls_cod subject_area_code\n" +
            ", cod.cls_des subject_area_name\n" +
            ", 'PROMIS' as subject_type_code\n" +
            ", 'Promis' as subject_type_name\n" +
            ", inf.work_type\n" +
            ", greatest(cls.inbound_ts, cod.inbound_ts) as inbound_ts\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmclst_current cls\n" +
            "inner join "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmclscodt_current cod on cls.cls_cod = cod.cls_cod\n" +
            "inner join "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_current inf on cls.pub_idt = inf.pub_idt) where U_KEY in ('%s')";

    public static String GET_PRICING_TransformMapping="select * from(select prc.pub_idt\n" +
            ", inf.epr_product_id as epr_id\n" +
            ", inf.product_type\n" +
            ", cast(prc.pub_idt as varchar)||cast(prc.pub_vol_idt as varchar)||std_cur_cod||prc.prc_typ||prc.prc_geo as u_key\n" +
            ", case when std_cur_cod = 'YPY' then 'JPY' when std_cur_cod = 'STG' then 'GBP' else std_cur_cod end as currency\n" +
            ", std_prc as price \n" +
            ", date(prc_dat) as start_date\n" +
            ", date(prc_dat + interval '1' year - interval '1' day) as end_date\n" +
            ", map.region_name as region\n" +
            ", 1 as quantity\n" +
            ", map.price_cust_cat_name as customer_category\n" +
            ", prc.inbound_ts\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpricest_current prc\n" +
            "inner join "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_current inf on prc.pub_idt = inf.pub_idt\n" +
            "inner join "+GetPRMDLDBUser.getPRMDataBase()+".promis_price_mapping map on prc.prc_typ = map.promis_id\n" +
            "where date(prc_dat) >= date('2010-01-01')\n" +
            "and inf.epr_product_id is not null) where U_KEY in ('%s')";

    public static String GET_PERSON_ROLES_TransformMapping="select * from (select aut.pub_idt\n" +
            ", inf.epr_id\n" +
            ", cast(aut.pub_idt as varchar)||cast(aut.aut_edt_idt as varchar) as u_key\n" +
            ", aut.typ_des as role_description\n" +
            ", aut.seq_num as sequence_number\n" +
            ", aut.aut_edt_typ as group_number\n" +
            ", aut.aut_edt_ini as initials\n" +
            ", aut.aut_edt_nam as last_name /*should this be called family name?*/\n" +
            ", aut.aut_edt_pre as title\n" +
            ", aut.aut_edt_suf as honours\n" +
            ", aut.aff_txt as affiliation\n" +
            ", replace(aut.bio_image,'&amp;','&') as image_url\n" +
            ", aut.ftn as footnote_txt\n" +
            ", aut.bio as notes_txt\n" +
            ", inf.work_type\n" +
            ", aut.inbound_ts\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmautpubt_current aut \n" +
            "inner join "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_current inf on aut.pub_idt = inf.pub_idt\n" +
            "where not (aut.typ_des is null and aut.aut_edt_nam is null and aut.aut_edt_ini is null)) where U_KEY in ('%s')";

    public static String GET_WORKS_TransformMapping="select * from (select inf.pub_idt\n" +
            ", inf.epr_id\n" +
            ", cast(inf.pub_idt as varchar) as u_key\n" +
            ", lon.lon_des as journal_aims_scope\n" +
            ", rel.rel_idt as elsevier_com_ind\n" +
            ", inf.aut_edt_nam as primary_author\n" +
            ", inf.for_tit as previous_title\n" +
            ", inf.work_type\n" +
            ", pmc.div_idt as internal_elsevier_division\n" +
            ", inf.inbound_ts\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_current inf\n" +
            "left outer join "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmlondest_current lon on inf.pub_idt = lon.pub_idt\n" +
            "left outer join "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubrelt_current rel on (inf.pub_idt = rel.pub_pub_idt and rel.rtp_rtp_cod = 'RDRCT')\n" +
            "left outer join "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmincpmct_current inc on inc.pub_idt = inf.pub_idt\n" +
            "left outer join "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpmccodt_current pmc on pmc.mkt_idt = inc.mkt_idt) where U_KEY in ('%s')";

    public static String GET_METRICS_TransformMapping="select * from (\n" +
            "select pub.pub_pub_idt as pub_idt\n" +
            ", inf.epr_id\n" +
            ", cast(pub.pub_pub_idt as varchar)||pub.rel_title||coalesce(pub.footnote,'') as u_key\n" +
            ", pub.rel_title as metric_code\n" +
            ", case when pub.rel_title = 'SNIP' then 'Source Normalized Impact per Paper'\n" +
            "       when pub.rel_title = 'SJR' then 'SCImago Journal Rank'\n" +
            "       else pub.rel_title end metric_name\n" +
            ", pub.rel_idt as metric\n" +
            ", case when length(pub.footnote) = 4 then try(cast(pub.footnote as integer)) end metric_year\n" +
            ", case when length(pub.footnote) != 4 then pub.footnote end metric_url\n" +
            ", inf.work_type\n" +
            ", inf.inbound_ts\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubrelt_current pub\n" +
            "inner join "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_current inf on pub.pub_pub_idt = inf.pub_idt\n" +
            "where pub.rtp_rtp_cod = 'BOXST'\n" +
            "union all\n" +
            "select pub_idt as pub_idt\n" +
            ", epr_id\n" +
            ", cast(pub_idt as varchar)||'IF'||cast(if_year as varchar) as u_key\n" +
            ", 'IF' as metric_code\n" +
            ", 'Impact Factor' as metric_name\n" +
            ", cast(if_no as varchar) as metric\n" +
            ", if_year as metric_year\n" +
            ", null as metric_url\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_current\n" +
            "where if_year is not null\n" +
            "union all\n" +
            "select pub_idt as pub_idt\n" +
            ", epr_id\n" +
            ", cast(pub_idt as varchar)||'IF5'||cast(if_year as varchar) as u_key\n" +
            ", 'IF5' as metric_code\n" +
            ", 'Impact Factor 5-year' as metric_name\n" +
            ", cast(if_5 as varchar) as metric\n" +
            ", if_year as metric_year\n" +
            ", null as metric_url\n" +
            ", work_type\n" +
            ", inbound_ts\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_current\n" +
            "where if_5 is not null) where U_KEY in ('%s')";

    public static String GET_URLS_TransformMapping="select * from(\n"+
            "select inf.pub_idt\n" +
            ", inf.epr_id\n" +
            ", cast(inf.pub_idt as varchar)||map.url_type_name||coalesce(rel_idt,'')||coalesce(rel_title,'') as u_key\n" +
            ", map.url_type_code as url_code\n" +
            ", map.url_type_name as url_name\n" +
            ", rel_idt as url\n" +
            ", rel_title as url_title\n" +
            ", inf.work_type\n" +
            ", rel.inbound_ts\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubrelt_current rel\n" +
            "inner join "+GetPRMDLDBUser.getPRMDataBase()+".promis_url_mapping map on rel.rtp_rtp_cod  = map.url_type_code\n" +
            "inner join "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_current inf ON rel.pub_pub_idt = inf.pub_idt\n" +
            "where rel_idt is not null) where U_KEY in ('%s')";

    public static String GET_WORK_RELS_TransformMapping="select * from (select\n" +
            "  par.pub_idt parent_pub_idt\n" +
            ", par.epr_id parent_epr_id\n" +
            ", cast(par.pub_idt as varchar)||cast(chd.pub_idt as varchar) as u_key\n" +
            ", chd.pub_idt child_pub_idt\n" +
            ", chd.epr_id child_epr_id\n" +
            ", w.work_title    child_title\n" +
            ", w.f_type        child_related_type_code\n" +
            ", t.l_description child_related_type_name\n" +
            ", t.roll_up_type  child_related_type_roll_up\n" +
            ", w.f_status      child_related_status_code\n" +
            ", s.l_description child_related_status_name\n" +
            ", s.roll_up_status  child_related_status_roll_up\n" +
            ", 'Includes' relationship_type_code\n" +
            ", 'Includes' relationship_type_name\n" +
            ", par.work_type\n" +
            ", par.inbound_ts\n" +
            "FROM "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubrelt_current rel\n" +
            "INNER JOIN "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_current par ON rel.pub_pub_idt = par.pub_idt\n" +
            "INNER JOIN "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_current chd ON rel.rel_idt = cast(chd.pub_idt as varchar)\n" +
            "INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_wwork w ON w.work_id = chd.epr_id\n" +
            "INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_type t ON t.code = w.f_type\n" +
            "INNER JOIN "+GetPRMDLDBUser.getProdDataBase()+".gd_x_lov_work_status s ON s.code = w.f_status\n" +
            "WHERE (rel.rtp_rtp_cod = 'JNPUB')) where U_KEY in ('%s')";

//  Latest and AllSource IDs SQL
    public static String GET_LATEST_PRICING_IDs = "select epr_id as EPR_ID from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_latest_pricing limit %s ";
    public static String GET_LATEST_WORKS_IDs = "select pub_idt as PUB_IDT from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_latest_works limit %s ";
    public static String GET_LATEST_METRICS_IDs = "select pub_idt as PUB_IDT from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_latest_metrics limit %s ";
    public static String GET_LATEST_PERSON_ROLES_IDs = "select pub_idt as PUB_IDT from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_latest_person_roles limit %s ";
    public static String GET_LATEST_WORK_RELS_IDs = "select parent_pub_idt as PARENT_PUB_IDT from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_latest_work_rels limit %s ";
    public static String GET_LATEST_SUBJECT_AREAS_IDs = "select pub_idt as PUB_IDT from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_latest_subject_areas limit %s ";
    public static String GET_LATEST_URLS_IDs = "select pub_idt as PUB_IDT from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_latest_subject_areas limit %s ";

    public static String GET_AllSource_Extended_Pricing = "select pub_idt as PUB_IDT from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_latest_subject_areas limit %s ";

}



