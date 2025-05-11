CREATE DEFINER=`permen`@`%` FUNCTION `dbmodularerp`.`in_check_qoh_available`() RETURNS tinyint(4)
BEGIN
	DECLARE check_qoh_available TINYINT;
	DECLARE get_stock_code VARCHAR(20);
	DECLARE get_kode_lokasi VARCHAR(50);
	DECLARE get_kode_perusahaan VARCHAR(50);
	DECLARE get_trans_date DATE;
	DECLARE get_quantity DOUBLE;
	DECLARE get_qoh_date DOUBLE;
	DECLARE get_qoh_all_date DOUBLE;
	DECLARE get_qoh DOUBLE;
	
	DECLARE done INT DEFAULT FALSE;
	DECLARE
		loop_line_check_qoh CURSOR FOR 
		SELECT 
		stock_code_temp, kode_lokasi_temp, kode_perusahaan_temp, trans_date_temp, quantity_temp
		FROM temp_check_qoh ; -- get data temp check qoh
	DECLARE CONTINUE HANDLER FOR NOT FOUND
		SET done = TRUE;
	
	this_procedure:BEGIN
		-- inisial value 1
		SET check_qoh_available = 1;
		-- loop
		OPEN loop_line_check_qoh;
		line_loop:LOOP
	    FETCH loop_line_check_qoh INTO  
			get_stock_code, 
			get_kode_lokasi,
			get_kode_perusahaan,
			get_trans_date,
			get_quantity;
		
			IF done THEN 
				LEAVE line_loop;
			END IF;
			
			-- cek qoh date
			SELECT dbmodular.in_get_qoh_on_date(get_kode_perusahaan, get_kode_lokasi, get_stock_code, get_trans_date, 0) INTO get_qoh_date;
		
			-- cek qoh all date
			SELECT dbmodular.in_get_qoh_on_date(get_kode_perusahaan, get_kode_lokasi, get_stock_code, '0000-00-00', 0) INTO get_qoh_all_date;
		
			-- seleksi qoh terbesar untuk dibandingkan
			IF get_qoh_date > get_qoh_all_date THEN
				SET get_qoh = get_qoh_date;
			ELSE
				SET get_qoh = get_qoh_all_date;
			END IF;
		
			-- validasi cek quantity
			IF get_quantity > get_qoh THEN
				SET check_qoh_available = 1;
				SET done = TRUE;
			END IF;
			
		END LOOP line_loop;
		CLOSE loop_line_check_qoh;
	END; -- end this_procedure begin
	
	
	RETURN check_qoh_available;
END