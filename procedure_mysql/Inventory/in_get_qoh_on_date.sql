CREATE DEFINER=`permen`@`%` FUNCTION `dbmodularerp`.`in_get_qoh_on_date`(in_kode_perusahaan VARCHAR(50), in_kode_lokasi VARCHAR(50), 
	in_stock_code VARCHAR(20), in_trans_date DATE, in_exclude_transit TINYINT) RETURNS double
BEGIN
	DECLARE get_qoh DOUBLE;

	if in_exclude_transit = 0 THEN 
		if in_trans_date = '0000-00-00' THEN
			select IFNULL( sum(qty),0) total_qty INTO get_qoh from in_03_t_stock_moves itsm 
			where 
			itsm.delete_mark = 0
			AND itsm.kode_perusahaan = in_kode_perusahaan
			AND itsm.stock_code = in_stock_code
			and itsm.kode_lokasi = in_kode_lokasi
			AND itsm.is_transit = 0;
		ELSE
			select IFNULL( sum(qty),0) total_qty INTO get_qoh from in_03_t_stock_moves itsm 
			where 
			itsm.delete_mark = 0
			AND itsm.kode_perusahaan = in_kode_perusahaan
			AND itsm.stock_code = in_stock_code
			and itsm.kode_lokasi = in_kode_lokasi
			and itsm.trans_date <= in_trans_date
			AND itsm.is_transit = 0;
		END IF;
	else 
		if in_trans_date = '0000-00-00' THEN
			select IFNULL( sum(qty),0) total_qty INTO get_qoh from in_03_t_stock_moves itsm 
			where 
			itsm.delete_mark = 0
			AND itsm.kode_perusahaan = in_kode_perusahaan
			AND itsm.stock_code = in_stock_code
			and itsm.kode_lokasi = in_kode_lokasi;
		ELSE
			select IFNULL( sum(qty),0) total_qty INTO get_qoh from in_03_t_stock_moves itsm 
			where 
			itsm.delete_mark = 0
			AND itsm.kode_perusahaan = in_kode_perusahaan
			AND itsm.stock_code = in_stock_code
			and itsm.kode_lokasi = in_kode_lokasi
			and itsm.trans_date <= in_trans_date;
		END IF;
	end if;
	RETURN get_qoh;
END