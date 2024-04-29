CREATE TABLE std6_52.ch_mart_6_52
(

    `date` date,
	`plant` String,
    `plant_name` String,
	`rpa` Float32,
	`discount` Float32,
    `rpa_with_discount` Float32,
	`qty` Int8,
	`qty_bills` Int8,
	`traffic` Int8,
	`qty_mat_promos` Int8,
	`percent_promo_mat` Float32,
	`av_material_in_bill` Float32,
	`koeff` Float32,
	`av_bill` Float32,
	`av_revenue` Float32
)
ENGINE = PostgreSQL('192.168.214.203:5432',
 adb,
 'mart_20210101',
 'std6_52',
 'ZS7Jfjcwf5og',
 'std6_52')
 