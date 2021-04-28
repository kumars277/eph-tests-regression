package com.eph.automation.testing.services.db.PROMISDataLakeSQL;

public class PromisETLCountChecksSQL {

    public static String GET_Promis_prmautpubt_part = "select count(*) as Total_Count from (select "
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
            +"FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s ppa)\n" +
            " where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts desc limit 1)";


    public static String GET_Promis_prmclscodt_part = "select count(*) as Total_Count from (select\n" +
            "  cls_cod,\n" +
            "  cls_des,\n" +
            "  inbound_ts\n" +
            " FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            " where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts desc limit 1)";

    public static String GET_Promis_prmclst_part = "select count(*) as Total_Count from (SELECT " +
            "ppp.pub_idt \n" +
            ",ppp.cls_cod \n" +
            ",cast(ppp.sca_cod as integer) priority \n" +
            ", ppp.inbound_ts \n" +
            " FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s ppp)\n" +
            " where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts desc limit 1)";

    public static String GET_Promis_prmlondest_part = "select count(*) as Total_Count from (SELECT\n" +
            " ppp.pub_idt \n" +
            ", ppp.pub_vol_idt \n" +
            ", ppp.vol_prt_idt \n" +
            ", ppp.lon_des \n" +
            ", ppp.inbound_ts \n" +
            "FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s ppp\n" +
            "  where pub_vol_idt = '0')" +
            " where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts desc limit 1)";

    public static String GET_Promis_prmpricest_part = "select count(*) as Total_Count from (SELECT\n" +
            "  ppp.pub_idt \n" +
            ", ppp.pub_vol_idt \n" +
            ", ppp.vol_prt_idt \n" +
            ", ppp.edn_idt \n" +
            ", ppp.prc_typ \n" +
            ", ppp.prc_geo \n" +
            ", ppp.ipt_cod \n" +
            ", ppp.std_cur_cod \n" +
            ", cast(ppp.std_prc as double) as std_prc \n" +
            ", ppp.prc_pre \n" +
            ", ppp.add_prc \n" +
            ", ppp.flag \n" +
            ", ppp.inbound_ts \n" +
            "FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s ppp) \n" +
            " where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts desc limit 1)";

    public static String GET_Promis_prmpubinft_part = "select count(*) as Total_Count from (SELECT \n" +
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
            "INNER JOIN "+GetPRMDLDBUser.getEphCrossRefDataBase()+".eph_identifier_cross_reference_v xw \n" +
            "  ON xw.identifier_type = 'ELSEVIER JOURNAL NUMBER' \n" +
            "  AND xw.record_level = 'Work' \n" +
            "  AND xw.identifier = ppp.jnl_idt \n" +
            "LEFT OUTER JOIN "+GetPRMDLDBUser.getEphCrossRefDataBase()+".eph_identifier_cross_reference_v xp \n" +
            "  ON xp.identifier_type = 'ELSEVIER JOURNAL NUMBER' \n" +
            "  AND xp.record_level = 'Product' \n" +
            "  AND xp.product_type = 'SUB' \n" +
            "  AND xp.manifestation_type = 'JPR' \n" +
            "  AND xp.identifier = ppp.jnl_idt \n" +
            "where source is null) " +
            " where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts desc limit 1)";

    public static String GET_Promis_prmpubrelt_part = "select count(*) as Total_Count from (SELECT\n" +
            "  ppp.pub_pub_idt \n" +
            ", ppp.rel_no \n" +
            ", ppp.rel_srt \n" +
            ", ppp.rel_idt \n" +
            ", ppp.rel_title \n" +
            ", ppp.footnote \n" +
            ", ppp.front_text \n" +
            ", ppp.end_text \n" +
            ", ppp.rtp_rtp_cod \n" +
            ", ppp.rel_end_date \n" +
            ", ppp.rel_start_date \n" +
            ", ppp.inbound_ts \n" +
            "FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s ppp \n" +
            "  WHERE ( \n" +
            "    (rtp_rtp_cod = 'BOXST' \n" +
            "    and ppp.pub_pub_idt||ppp.rtp_rtp_cod||ppp.rel_title||ppp.rel_no IN \n" +
            "     (SELECT pp1.pub_pub_idt||pp1.rtp_rtp_cod||pp1.rel_title||max(pp1.rel_no) \n" +
            "      FROM "+GetPRMDLDBUser.getPRMDataBase()+".%s pp1 \n" +
            "      WHERE rtp_rtp_cod = 'BOXST' \n" +
            "      GROUP BY pp1.pub_pub_idt||pp1.rtp_rtp_cod||pp1.rel_title) \n" +
            "    ) \n" +
            "    or (rtp_rtp_cod <> 'BOXST')))" +
            " where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts desc limit 1)";

    public static String GET_Promis_prmincpmct_part = "SELECT count(*) as Total_Count from (select\n" +
            "  pub_idt,\n" +
            "  mkt_idt,\n" +
            "  mkt_sub_idt,\n" +
            "  inbound_ts\n" +
            "FROM "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmincpmct_part)" +
            " where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts desc limit 1)";

    public static String GET_Promis_prmpmccodt_part = "SELECT count(*) as Total_Count from (select\n" +
            "  mkt_idt,\n" +
            "  mkt_des,\n" +
            "  div_idt,\n" +
            "  inbound_ts\n" +
            "FROM "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpmccodt_part)" +
            " where inbound_ts = (select inbound_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s order by inbound_ts desc limit 1)";


    public static String GET_Promis_Current = "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".%s\n";

    public static String GET_Promis_PreviousHistory =  "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n"
        + "where transform_ts = (select distinct transform_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s where transform_ts < (select distinct transform_ts from "+GetPRMDLDBUser.getPRMDataBase() +".%s order by transform_ts desc limit 1)order by transform_ts desc limit 1)";

    public static String GET_Promis_Previous = "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".%s";

    public static String GET_Promis_Subject_Areas_DeltaQuery = "select count(*) as Total_Count from (with crr_dataset as( \n" +
            "  select pub_idt,epr_id,u_key,priority,subject_area_code,subject_area_name,subject_type_code,subject_type_name,work_type,transform_file_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select pub_idt,epr_id,u_key,priority,subject_area_code,subject_area_name,subject_type_code,subject_type_name,work_type,transform_file_ts\n" +
            "  from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".%s dhap\n" +
            ") where rn = 2)) \n" +
            "select 'new' , * \n" +
            "from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where prev.u_key is null\n" +
            "    union\n" +
            "select 'deleted', * \n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', * \n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where (coalesce(crr.pub_idt, null) <> coalesce(prev.pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
            "coalesce(crr.epr_id, 'null') <> coalesce(prev.epr_id, 'null') or\n" +
            "coalesce(crr.priority, null) <> coalesce(prev.priority, null) or\n" +
            "coalesce(crr.subject_area_code, 'null') <> coalesce(prev.subject_area_code, 'null') or\n" +
            "coalesce(crr.subject_area_name, 'null') <> coalesce(prev.subject_area_name, 'null') or\n" +
            "coalesce(crr.subject_type_code, 'null') <> coalesce(prev.subject_type_code, 'null') or\n" +
            "coalesce(crr.subject_type_name, 'null') <> coalesce(prev.subject_type_name, 'null') or\n" +
            "coalesce(crr.work_type, 'null') <> coalesce(prev.work_type, 'null')))";

    public static String GET_Promis_Pricing_DeltaQuery = "select count(*) as Total_Count from (with crr_dataset as( \n" +
            "  select pub_idt,epr_id,product_type,u_key,currency,price,start_date,end_date,region,quantity,customer_category,transform_file_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select pub_idt,epr_id,product_type,u_key,currency,price,start_date,end_date,region,quantity,customer_category,transform_file_ts\n" +
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
            "left join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where prev.u_key is null\n" +
            "    union\n" +
            "select 'deleted', *\n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', *\n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where (coalesce(crr.pub_idt, null) <> coalesce(prev.pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
            "coalesce(crr.epr_id, 'null') <> coalesce(prev.epr_id, 'null') or\n" +
            "coalesce(crr.product_type, 'null') <> coalesce(prev.product_type, 'null') or\n" +
            "coalesce(crr.currency, 'null') <> coalesce(prev.currency, 'null') or\n" +
            "coalesce(crr.price, null) <> coalesce(prev.price, null) or\n" +
            "coalesce(crr.start_date, null) <> coalesce(prev.start_date, null) or\n" +
            "coalesce(crr.end_date, null) <> coalesce(prev.end_date, null) or\n" +
            "coalesce(crr.region, 'null') <> coalesce(prev.region, 'null') or\n" +
            "coalesce(crr.quantity, null) <> coalesce(prev.quantity, null) or\n" +
            "coalesce(crr.customer_category, 'null') <> coalesce(prev.customer_category, 'null')))";

    public static String GET_Promis_Person_Roles_DeltaQuery = "select count(*) as Total_Count from (with crr_dataset as( \n" +
            "  select pub_idt,epr_id,u_key,role_description,sequence_number,group_number,initials,last_name,title,honours,affiliation,image_url,footnote_txt,notes_txt,work_type,transform_file_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select pub_idt,epr_id,u_key,role_description,sequence_number,group_number,initials,last_name,title,honours,affiliation,image_url,footnote_txt,notes_txt,work_type,transform_file_ts\n" +
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
            "left join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where prev.u_key is null\n" +
            "    union\n" +
            "select 'deleted', *\n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', *\n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where (coalesce(crr.pub_idt, null) <> coalesce(prev.pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
            "coalesce(crr.epr_id, 'null') <> coalesce(prev.epr_id, 'null') or\n" +
            "coalesce(crr.role_description, 'null') <> coalesce(prev.role_description, 'null') or\n" +
            "coalesce(crr.sequence_number, null) <> coalesce(prev.sequence_number, null) or\n" +
            "coalesce(crr.group_number, null) <> coalesce(prev.group_number, null) or\n" +
            "coalesce(crr.initials, 'null') <> coalesce(prev.initials, 'null') or\n" +
            "coalesce(crr.last_name, 'null') <> coalesce(prev.last_name, 'null') or\n" +
            "coalesce(crr.title, 'null') <> coalesce(prev.title, 'null') or\n" +
            "coalesce(crr.honours, 'null') <> coalesce(prev.honours, 'null') or\n" +
            "coalesce(crr.affiliation, 'null') <> coalesce(prev.affiliation, 'null') or \n" +
            "coalesce(crr.image_url, 'null') <> coalesce(prev.image_url, 'null') or \n" +
            "coalesce(crr.footnote_txt, 'null') <> coalesce(prev.footnote_txt, 'null') or \n" +
            "coalesce(crr.notes_txt, 'null') <> coalesce(prev.notes_txt, 'null') or \n" +
            "coalesce(crr.work_type, 'null') <> coalesce(prev.work_type, 'null')))";

    public static String GET_Promis_Works_DeltaQuery = "select count(*) as Total_Count from (with crr_dataset as( \n" +
            "  select pub_idt,epr_id,u_key,journal_aims_scope,elsevier_com_ind,primary_author,previous_title,internal_elsevier_division,work_type,transform_file_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select pub_idt,epr_id,u_key,journal_aims_scope,elsevier_com_ind,primary_author,previous_title,internal_elsevier_division,work_type,transform_file_ts\n" +
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
            "left join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where prev.u_key is null\n" +
            "    union\n" +
            "select 'deleted', *\n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', *\n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where (coalesce(crr.pub_idt, null) <> coalesce(prev.pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
            "coalesce(crr.epr_id, 'null') <> coalesce(prev.epr_id, 'null') or\n" +
            "coalesce(crr.journal_aims_scope, 'null') <> coalesce(prev.journal_aims_scope, 'null') or\n" +
            "coalesce(crr.elsevier_com_ind, null) <> coalesce(prev.elsevier_com_ind, null) or\n" +
            "coalesce(crr.primary_author, null) <> coalesce(prev.primary_author, null) or\n" +
            "coalesce(crr.previous_title, 'null') <> coalesce(prev.previous_title, 'null') or\n" +
            "coalesce(crr.internal_elsevier_division, 'null') <> coalesce(prev.internal_elsevier_division, 'null') or\n" +
            "coalesce(crr.work_type, 'null') <> coalesce(prev.work_type, 'null')))";

    public static String GET_Promis_Metrics_DeltaQuery = "select count(*) as Total_Count from (with crr_dataset as( \n" +
            "  select pub_idt,epr_id,u_key,metric_code,metric_name,metric,metric_year,metric_url,work_type,transform_file_ts from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from "+GetPRMDLDBUser.getPRMDataBase()+".%s)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select pub_idt,epr_id,u_key,metric_code,metric_name,metric,metric_year,metric_url,work_type,transform_file_ts\n" +
            "  from "+GetPRMDLDBUser.getPRMDataBase()+".%s \n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn \n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".%s dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ") \n" +
            "select 'new', * \n" +
            "from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where prev.u_key is null\n" +
            "    union\n" +
            "select 'deleted', * \n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', * \n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where (coalesce(crr.pub_idt, null) <> coalesce(prev.pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
            "coalesce(crr.epr_id, 'null') <> coalesce(prev.epr_id, 'null') or\n" +
            "coalesce(crr.metric_code, 'null') <> coalesce(prev.metric_code, 'null') or\n" +
            "coalesce(crr.metric_name, 'null') <> coalesce(prev.metric_name, 'null') or\n" +
            "coalesce(crr.metric, null) <> coalesce(prev.metric, null) or\n" +
            "coalesce(crr.metric_year, null) <> coalesce(prev.metric_year, null) or\n" +
            "coalesce(crr.metric_url, 'null') <> coalesce(prev.metric_url, 'null') or\n" +
            "coalesce(crr.work_type, 'null') <> coalesce(prev.work_type, 'null')))";

    public static String GET_Promis_Urls_DeltaQuery = "select count(*) as Total_Count from (with crr_dataset as( \n" +
            "  select pub_idt,epr_id,u_key,url_code,url_name,url,url_title,work_type,transform_file_ts from promis_staging_sit.promis_transform_file_history_urls_part \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from promis_staging_sit.promis_transform_file_history_urls_part)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select pub_idt,epr_id,u_key,url_code,url_name,url,url_title,work_type,transform_file_ts\n" +
            "  from promis_staging_sit.promis_transform_file_history_urls_part\n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn \n" +
            "from promis_staging_sit.promis_transform_file_history_urls_part dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ") \n" +
            "select 'new', * \n" +
            "from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where prev.u_key is null\n" +
            "    union\n" +
            "select 'deleted', * \n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', * \n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where (coalesce(crr.pub_idt, null) <> coalesce(prev.pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
            "coalesce(crr.epr_id, 'null') <> coalesce(prev.epr_id, 'null') or\n" +
            "coalesce(crr.url_code, 'null') <> coalesce(prev.url_code, 'null') or\n" +
            "coalesce(crr.url_name, 'null') <> coalesce(prev.url_name, 'null') or\n" +
            "coalesce(crr.url, 'null') <> coalesce(prev.url, 'null') or\n" +
            "coalesce(crr.url_title, 'null') <> coalesce(prev.url_title, 'null') or\n" +
            "coalesce(crr.work_type, 'null') <> coalesce(prev.work_type, 'null')))";

    public static String GET_Promis_Work_Rels_DeltaQuery = "select count(*) as Total_Count from (with crr_dataset as( \n" +
            "  select parent_pub_idt,parent_epr_id,u_key,child_pub_idt,child_epr_id,child_title,child_related_type_code,child_related_type_name,child_related_type_roll_up,child_related_status_code,child_related_status_name,child_related_status_roll_up,relationship_type_code,relationship_type_name,work_type,transform_file_ts from promis_staging_sit.promis_transform_file_history_work_rels_part \n" +
            "  where transform_file_ts = (select max(transform_file_ts ) from promis_staging_sit.promis_transform_file_history_work_rels_part)\n" +
            "  ),\n" +
            "  prev_dataset as (\n" +
            "  select parent_pub_idt,parent_epr_id,u_key,child_pub_idt,child_epr_id,child_title,child_related_type_code,child_related_type_name,child_related_type_roll_up,child_related_status_code,child_related_status_name,child_related_status_roll_up,relationship_type_code,relationship_type_name,work_type,transform_file_ts\n" +
            "  from promis_staging_sit.promis_transform_file_history_work_rels_part\n" +
            "  where transform_file_ts = (select distinct transform_file_ts\n" +
            "from (select dhap.*, dense_rank() over (order by transform_file_ts desc) as rn \n" +
            "from promis_staging_sit.promis_transform_file_history_work_rels_part dhap\n" +
            ")\n" +
            "where rn = 2\n" +
            ")\n" +
            ") \n" +
            "select 'new', * \n" +
            "from crr_dataset crr\n" +
            "left join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where prev.u_key is null\n" +
            "    union\n" +
            "select 'deleted', * \n" +
            "from prev_dataset prev\n" +
            "left join crr_dataset crr on crr.u_key = prev.u_key\n" +
            "where crr.u_key is null\n" +
            "    union\n" +
            "select 'changed', * \n" +
            "from crr_dataset crr\n" +
            "join prev_dataset prev on crr.u_key = prev.u_key\n" +
            "where (coalesce(crr.parent_pub_idt, null) <> coalesce(prev.parent_pub_idt, null) or\n" +
            "coalesce(crr.u_key, 'null') <> coalesce(prev.u_key, 'null') or\n" +
            "coalesce(crr.parent_epr_id, 'null') <> coalesce(prev.parent_epr_id, 'null') or\n" +
            "coalesce(crr.child_pub_idt, null) <> coalesce(prev.child_pub_idt, null) or\n" +
            "coalesce(crr.child_epr_id, 'null') <> coalesce(prev.child_epr_id, 'null') or\n" +
            "coalesce(crr.child_title, 'null') <> coalesce(prev.child_title, 'null') or\n" +
            "coalesce(crr.child_related_type_code, 'null') <> coalesce(prev.child_related_type_code, 'null') or\n" +
            "coalesce(crr.child_related_type_roll_up, 'null') <> coalesce(prev.child_related_type_roll_up, 'null') or\n" +
            "coalesce(crr.child_related_status_code, 'null') <> coalesce(prev.child_related_status_code, 'null') or\n" +
            "coalesce(crr.child_related_status_name, 'null') <> coalesce(prev.child_related_status_name, 'null') or\n" +
            "coalesce(crr.child_related_status_roll_up, 'null') <> coalesce(prev.child_related_status_roll_up, 'null') or\n" +
            "coalesce(crr.relationship_type_code, 'null') <> coalesce(prev.relationship_type_code, 'null') or\n" +
            "coalesce(crr.relationship_type_name, 'null') <> coalesce(prev.relationship_type_name, 'null') or\n" +
            "coalesce(crr.work_type, 'null') <> coalesce(prev.work_type, 'null') or\n" +
            "coalesce(crr.child_related_type_name, 'null') <> coalesce(prev.child_related_type_name, 'null')))";


    public static String GET_Promis_Delta = "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".%s";

    public static String GET_Promis_Subject_Areas_HistExclQuery = "select count(*) as Total_Count from ( select\n" +
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
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_subject_areas))";

    public static String GET_Promis_Pricing_HistExclQuery = "select count(*) as Total_Count from (select\n" +
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
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_pricing))";

    public static String GET_Promis_Person_Roles_HistExclQuery ="select count(*) as Total_Count from(\n" +
            "select\n" +
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
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_person_roles))";

    public static String GET_Promis_Works_HistExclQuery ="select count(*) as Total_Count from(\n" +
            "select\n" +
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
            ", delete_flag\n" +
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_works_part tdh\n" +
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_works))";

    public static String GET_Promis_Metrics_HistExclQuery ="select count(*) as Total_Count from (\n" +
            "select\n" +
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
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_metrics))";

    public static String GET_Promis_Urls_HistExclQuery ="select count(*) as Total_Count from(\n" +
            "select\n" +
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
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_urls))";

    public static String GET_Promis_Work_Rels_HistExclQuery ="select count(*) as Total_Count from(\n" +
            "select\n" +
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
            "where u_key not in (select u_key from "+GetPRMDLDBUser.getPRMDataBase()+".promis_delta_current_work_rels))";

    public static String GET_Promis_History_Excluding="select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".%s";

    public static String GET_Promis_Subject_Areas_LatestQuery="select count(*) as Total_Count from(\n" +
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
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_subject_areas_excl_delta)";

    public static String GET_Promis_Pricing_LatestQuery="select count(*) as Total_Count from(\n" +
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
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_pricing_excl_delta)";

    public static String GET_Promis_Person_Roles_LatestQuery="select count(*) as Total_Count from(\n" +
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
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_person_roles_excl_delta)";

    public static String GET_Promis_Works_LatestQuery="select count(*) as Total_Count from(\n" +
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
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_person_roles_excl_delta)";

    public static String GET_Promis_Metrics_LatestQuery="select count(*) as Total_Count from(\n" +
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
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_metrics_excl_delta)";

    public static String GET_Promis_Urls_LatestQuery="select count(*) as Total_Count from(\n" +
            "select  pub_idt\n" +
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
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_urls_excl_delta)";

    public static String GET_Promis_Work_Rels_LatestQuery="select count(*) as Total_Count from(\n" +
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
            "from "+GetPRMDLDBUser.getPRMDataBase()+".promis_transform_history_work_rels_excl_delta)";

    public static String GET_Promis_Latest_Excluding= "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".%s";

    public static String GET_Promis_Latest= "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".%s";

    public static String GET_Promis_AllSource= "select count(*) as Total_Count from "+GetPRMDLDBUser.getProdStagingDataBase()+".%s where \"source\"='PROMIS'";

    public static String GET_Promis_pricing_Latest= "select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".%s where \"end_date\" is null or \"end_date\" >= current_date";

    public static String GET_Promis_Subject_Areas_TransformMapping = "select count(*) as Total_Count from (select cls.pub_idt\n" +
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
            "inner join "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpubinft_current inf on cls.pub_idt = inf.pub_idt)";

    public static String GET_Promis_Pricing_TransformMapping = "select count(*) as Total_Count from(select prc.pub_idt\n" +
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
            "and inf.epr_product_id is not null)";

    public static String GET_Promis_Person_Roles_TransformMapping = "select count(*) as Total_Count from (select aut.pub_idt\n" +
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
            "where not (aut.typ_des is null and aut.aut_edt_nam is null and aut.aut_edt_ini is null))";

    public static String GET_Promis_Works_TransformMapping = "select count(*) as Total_Count from (select inf.pub_idt\n" +
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
            "left outer join "+GetPRMDLDBUser.getPRMDataBase()+".promis_prmpmccodt_current pmc on pmc.mkt_idt = inc.mkt_idt)";

    public static String GET_Promis_Metrics_TransformMapping ="select count(*) as Total_Count from (\n" +
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
            "where if_5 is not null)";

    public static String GET_Promis_Urls_TransformMapping="select count(*) as Total_Count from(\n"+
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
            "where rel_idt is not null)";

    public static String GET_Promis_Work_Rels_TransformMapping= "select count(*) as Total_Count from (select\n" +
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
            "WHERE (rel.rtp_rtp_cod = 'JNPUB'))";

    public static String GET_Promis_Transform_Current="select count(*) as Total_Count from "+GetPRMDLDBUser.getPRMDataBase()+".%s";

}
