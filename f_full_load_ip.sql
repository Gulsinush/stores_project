CREATE OR REPLACE FUNCTION std6_52.f_load_full_IP(p_table text, p_file_name text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

DECLARE 

v_ext_table_name text;
v_sql text;
v_gpfdist text;
v_result text;

BEGIN -- начало тела функции

v_ext_table_name = p_table||'_ext'; --возвращаемое значение функции

execute 'TRUNCATE TABLE '||p_table;

execute 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table_name;

v_gpfdist = 'gpfdist://172.16.128.186:8080/'||p_file_name||'.csv';

v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table_name||'(LIKE '||p_table||')
LOCATION ('''||v_gpfdist||''') on ALL
FORMAT ''CSV''(HEADER DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'')
ENCODING ''UTF8''
SEGMENT REJECT LIMIT 10 ROWS';

raise notice 'EXTERNAL TABLE IS: %', v_sql;

execute v_sql;

execute 'INSERT INTO '||p_table||' SELECT * FROM '||v_ext_table_name;

execute 'SELECT COUNT(*) FROM '||p_table into v_result;

return v_result;

END; -- конец тела функции

$$
EXECUTE ON ANY;



SELECT std6_52.f_load_full_ip('stores', 'stores')

SELECT std6_52.f_load_full_ip('coupons', 'coupons')

SELECT std6_52.f_load_full_ip('promos', 'promos')

SELECT std6_52.f_load_full_ip('promo_types', 'promo_types')