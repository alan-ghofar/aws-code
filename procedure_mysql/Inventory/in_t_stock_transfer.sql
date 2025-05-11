CREATE DEFINER=`permen`@`%` PROCEDURE `in_t_stock_transfer`(in_id_menu INT, in_kode_perusahaan VARCHAR(50), in_type_doc VARCHAR(100), in_from_loc VARCHAR(50), in_to_loc VARCHAR(50), in_no_asset VARCHAR(255), in_isship INT(1), in_status_transfer INT(5), in_doc_ref VARCHAR(100), in_no_ref INT(11),in_trans_date DATE, in_memo TEXT, in_status_code VARCHAR(50), in_create_by INT, in_line_items TEXT, in_trans_no INT)
BEGIN
	DECLARE get_nama_menu VARCHAR(255);
	DECLARE get_kode_lokasi VARCHAR(50);
	DECLARE get_trans_no INT;
	DECLARE get_reference VARCHAR(100);	
	DECLARE get_uom VARCHAR(100);
-- 	DECLARE get_coa_adj VARCHAR(15);
	DECLARE hasil_line_item VARCHAR(100);
	DECLARE total_amount DOUBLE;
	DECLARE lokasi_detail VARCHAR(50);
	DECLARE sql_line_items TEXT;
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION     
		BEGIN          
		  SELECT 'failed';
			ROLLBACK;
		END;
	
	START TRANSACTION;

-- 	in_trans_no = 0 ketika transaksi baru
	IF(in_no_ref > 0) THEN
		set lokasi_detail = in_to_loc;
	ELSE
		SET lokasi_detail = in_from_loc;
	END IF;

	IF (in_trans_no = 0) THEN
		SET @delim = "=+=";
		SET @delim2 = "=-=";

		SET sql_line_items = REPLACE(in_line_items,@delim,"\'),(\'");
		SET sql_line_items = REPLACE(sql_line_items,@delim2,"\',\'");

		DROP TABLE IF EXISTS temp_line_items;
		CREATE TEMPORARY TABLE temp_line_items (id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, item_code_temp TEXT, item_name_temp TEXT, quantity_temp DOUBLE, unit_price_temp DOUBLE, uom_temp VARCHAR(255), memo_line_temp TEXT, id_line_temp INT );

		SET @sqlx := CONCAT('INSERT INTO temp_line_items (item_code_temp, item_name_temp, quantity_temp, unit_price_temp, uom_temp, memo_line_temp, id_line_temp) VALUES (',  "\'",sql_line_items, "\'" , ')');
		PREPARE myStmt FROM @sqlx;
		EXECUTE myStmt;
		SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
		-- SELECT unit_code INTO get_uom FROM in_01_m_item_codes WHERE item_code = 
		update temp_line_items lineitm left join in_03_m_stock_master stk on (lineitm.item_code_temp = stk.stock_code)
		set lineitm.uom_temp = stk.unit_code;

		-- 	SELECT coa INTO get_coa_adj FROM in_01_m_material_issue_types WHERE id = adj_type;

		SET get_trans_no = `generate_document_number`(in_kode_perusahaan, in_type_doc, in_trans_date , in_create_by);

		SELECT refs INTO get_reference FROM st_02_refs WHERE type_doc = in_type_doc AND trans_no = get_trans_no LIMIT 1;

		-- ambil dari price x qty line itemnya
		SELECT SUM(unit_price_temp * quantity_temp) as total_amount INTO total_amount FROM temp_line_items;

		-- 		SELECT in_type_doc, get_trans_no, in_status_code, in_create_by;
		INSERT INTO in_01_t_location_transfer (trans_no,
		 type_doc,
		 trans_date,
		 from_loc,
		 to_loc,
		 no_asset,
		 isship,
		 status_code,
		 status_transfer,
		 memo,
		 doc_ref,
		 no_ref,
		 to_cabang,
		 from_cabang,
		 create_by,
		 create_date,
		 kode_perusahaan,
		 reference)
		VALUES (
		get_trans_no,
		 in_type_doc,
		 in_trans_date,
		 in_from_loc,
		 in_to_loc,
		 in_no_asset,
		 in_isship,
		 in_status_code,
		 in_status_transfer,
		 in_memo,
		 in_doc_ref,
		 in_no_ref,
		 0,
		 0,
		 in_create_by,
		 CURDATE(),
		 in_kode_perusahaan,
		 get_reference);

		INSERT INTO in_02_t_location_transfer_detail ( trans_no, type_doc, stock_code, item_name, quantity, unit_price, item_unit, kode_lokasi, kode_perusahaan, memo, src_id)
		SELECT get_trans_no, in_type_doc, temp.item_code_temp, temp.item_name_temp, temp.quantity_temp, temp.unit_price_temp, temp.uom_temp, lokasi_detail, in_kode_perusahaan, temp.memo_line_temp, temp.id_line_temp FROM temp_line_items temp;
		
		IF(in_no_ref > 0) THEN
			
			update in_01_t_location_transfer ltd 
			set ltd.dispatched =  1 
			where ltd.type_doc = in_doc_ref and ltd.trans_no = in_no_ref;
			
			update in_02_t_location_transfer_detail  ltd 
			left join temp_line_items temp on (ltd.id = temp.id_line_temp)
			set ltd.quantity_sent =  temp.quantity_temp 
			where ltd.type_doc = in_doc_ref and ltd.trans_no = in_no_ref;
			
		END IF;
		
		SET get_kode_lokasi = in_from_loc;
		ELSE
			SET get_trans_no = in_trans_no;
			SELECT kode_lokasi INTO get_kode_lokasi FROM in_01_t_adjustment WHERE type_doc = in_type_doc AND trans_no = in_trans_no;
			SELECT refs INTO get_reference FROM st_02_refs WHERE type_doc = in_type_doc AND trans_no = get_trans_no LIMIT 1;
			
			-- get nama menu from table menu
			SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
			
			CALL update_status_code_document_transaction(in_type_doc, get_trans_no, in_status_code, '', in_create_by);
		
	END IF;
	-- insert ke table document workflow jika status_code = 2
	CALL st_document_workflow_log(in_type_doc, get_trans_no, in_status_code, in_create_by, in_kode_perusahaan, get_kode_lokasi);
	
	
	-- insert ke audit trail
	CALL add_audit_trail(in_type_doc, get_trans_no, in_create_by, in_id_menu, get_nama_menu, 'ADD', '', in_kode_perusahaan);
	  
	COMMIT;
	
	SELECT get_trans_no, get_reference;
	
END