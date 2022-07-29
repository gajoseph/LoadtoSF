/*
Drop the split table 
*/
drop table qlik_app_qvd_fld_split cascade;
create table qlik_app_qvd_fld_split as 
select *, regexp_split_to_table(qlik_qvd_fld, ', |-') as qlik_qvd_fld_split from qlik_app_qvd_fld;
-- create qvd_lineage this create a lineage from local qvd to actula qvd file
drop table qvd_lineage;
create table qvd_lineage as 
WITH 
	RECURSIVE qlik_app_qvd_fld_lvl (qlik_app_name ,qlik_tab_name ,
    qlik_app_local_qvd_name ,
    qlik_qvd_name ,
    qlik_qvd_col , qlik_qvd_fld,
	qlik_qvd_fld_split_par, qlik_qvd_fld_split_chld,
	qlik_qvd_col_alias, level1, qvd_path, col_path ) AS (
  SELECT qlik_app_name ,qlik_tab_name::text ,
    qlik_app_local_qvd_name ,
    replace(split_part(upper( qlik_qvd_name ), '/', -1), '.QVD]', '') ::text,
    qlik_qvd_col , --regexp_split_to_table(qlik_qvd_col, '&'),
	qlik_qvd_fld,
	null, qlik_qvd_fld_split,
	qlik_qvd_col_alias, 0 ,  Array[qlik_qvd_name]::text[]  , --Array[qlik_qvd_name||'.'||qlik_qvd_fld_split]::text[]
	json_build_object('qvd', qlik_qvd_name, 'qvd_col',qlik_qvd_fld_split )::jsonb as col_path
  FROM qlik_app_qvd_fld_split where qlik_qvd_name like '%.qvd%' --and qlik_qvd_col_alias = 'MDM_Key'
  UNION
  SELECT  p.qlik_app_name,  c.qlik_tab_name ,
    c.qlik_app_local_qvd_name ,
    p.qlik_qvd_name ,
  	c.qlik_qvd_col ,
	c.qlik_qvd_fld , 
	p.qlik_qvd_fld_split_chld, c.qlik_qvd_fld_split,
	c.qlik_qvd_col_alias, p.level1::int+1, array_append(  p.qvd_path::varchar(255)[], c.qlik_qvd_name) ,
	case when p.qlik_qvd_col_alias like '%'||c.qlik_qvd_fld_split||'%' then 
		json_build_object('qvd', c.qlik_qvd_name, 'qvd_col',p.qlik_qvd_col_alias, 'parent',col_path )::jsonb
		else col_path
		end
  FROM qlik_app_qvd_fld_lvl p
  left JOIN qlik_app_qvd_fld_split c ON ( p.qlik_app_local_qvd_name like '%'||c.qlik_qvd_name||'%') 
		and p.qlik_qvd_col_alias  =c.qlik_qvd_fld_split
		where  c.qlik_app_local_qvd_name is not null
  )
SELECT * FROM qlik_app_qvd_fld_lvl a ;

/*
Now we create qvd to db lineage; local qvd to qvd file is created in aboev step
*/

drop table if exists qvd_db_lineage; 
create table qvd_db_lineage as 
select a.qlik_app_name, a.qlik_tab_name, a.qlik_app_local_qvd_name, a.qvd_path,qlik_qvd_fld_split_chld as qlik_qvd_fld, qlik_qvd_col
					, qlik_qvd_fld_split_par, qlik_qvd_fld as qlik_qvd_fld_arr, qlik_qvd_col_alias
					, col_path, a.qlik_qvd_fld_split_chld
					, tc.qlik_app_name as qvd_gen_app
					, tc.qlik_tab_name as qvd_gen_app_tab_name
					, tc.qlik_conn_name
					, tc.qlik_qvd_name as qlik_qvd_name
			 		, tc.db_tab_name as src_data_entity, tc.db_col_name as src_col_name
from qvd_lineage a 
 	   join qvd_tab_columns tc on tc.qlik_qvd_name = a.qlik_qvd_name 
 		and ( ( trim(tc.db_col_name) like '%'|| trim(a.qlik_qvd_fld_split_PAR)||'%' or trim(a.qlik_qvd_fld_split_PAR) like '%'|| trim(tc.db_col_name)||'%'
 			 or trim(tc.db_col_name) = trim(a.qlik_qvd_fld_split_chld)) 
 			 or
			 (trim(tc.db_col_name) like '%'|| trim(a.qlik_qvd_fld_split_chld)||'%' or trim(a.qlik_qvd_fld_split_chld) like '%'|| trim(tc.db_col_name)||'%'
 			) ) 
;
/*
explain (format json)
final_

*/
--- FINAL query joins the control to qvd_db_lineage and to qvd_db_columns to get xls file info
select  c.qlik_app_id, c.qlik_app_name, c.sheet_name, c.control_type, c.control_title, c.control_fld_label, c.control_fld_field,
a.qlik_tab_name, a.qlik_app_local_qvd_name, a.qvd_path,qlik_qvd_fld_split_chld as qlik_qvd_fld, qlik_qvd_col, qlik_qvd_fld_split_par, qlik_qvd_fld as qlik_qvd_fld_arr, qlik_qvd_col_alias
, col_path, a.qlik_qvd_fld_split_chld
, COALESCE(a.qvd_gen_app,dc.qlik_app_name)   as qvd_gen_app
, COALESCE(a.qvd_gen_app_tab_name,dc.qlik_tab_name)   as qvd_gen_app_tab_name
, COALESCE(a.qlik_conn_name,dc.qlik_conn_name)   as qlik_conn_name

, COALESCE(a.qlik_qvd_name,dc.qlik_qvd_name)  	as qlik_qvd_name 
, COALESCE(a.src_data_entity, dc.db_tab_name) as src_data_entity
, COALESCE(a.src_col_name,dc.db_col_name)  as src_col_name
from qlik_app_sheet_controls c 
  left join qvd_db_lineage A
	on  control_fld_field  like '%' || trim( a.qlik_qvd_col_alias) ||'%' 
 	and c.qlik_app_name like a.qlik_app_name||'%' 

left join qvd_tab_columns dc on  c.qlik_app_name like dc.qlik_app_name||'%' 
		and (c.control_fld_field like '%'||dc.db_col_name||'%' 
			 or dc.db_col_name like '%'||c.control_fld_field||'%' ) 
where 1=1 --and(a.qlik_tab_name) like 'Lead Time 2%' 
	--and c.qlik_app_name like 'Catalog Performance Tracker%'
	--and sheet_name like '1-3 Month Supply' 
	--and COALESCE(a.src_col_name,dc.db_col_name) is not null
	--and c.control_fld_field = 'Count([PO Line Number])'
	and control_type not in ('gp-swr-sense-navigation' , 'filterpane') and trim(control_fld_field) !='';

