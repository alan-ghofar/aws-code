CREATE DEFINER=`permen`@`%` PROCEDURE `in_t_adjustments`(in_id_menu INT, in_kode_perusahaan VARCHAR(50), in_type_doc VARCHAR(100), jenis_adj INT, adj_type INT, in_kode_lokasi VARCHAR(50), in_trans_date DATE, in_memo TEXT, in_status_code VARCHAR(50), in_create_by INT, in_line_items TEXT, in_trans_no INT)
BEGIN
	DECLARE get_nama_menu VARCHAR(255);
	DECLARE get_kode_lokasi VARCHAR(50);
	DECLARE get_trans_no INT;
	DECLARE get_reference VARCHAR(100);	
	DECLARE get_uom VARCHAR(100);
	DECLARE get_coa_adj VARCHAR(15);
	DECLARE hasil_line_item VARCHAR(100);
	DECLARE total_amount DOUBLE;
	DECLARE sql_line_items TEXT;
 	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION     
		BEGIN          
		  SELECT 'failed';
			ROLLBACK;
		END;
	
	START TRANSACTION;

	this_procedure:BEGIN
-- 	in_trans_no = 0 ketika transaksi baru
	IF (in_trans_no = 0) THEN
	SET @delim = "=+=";
	SET @delim2 = "=-=";
	
	SET sql_line_items = REPLACE(in_line_items,@delim,"\'),(\'");
	SET sql_line_items = REPLACE(sql_line_items,@delim2,"\',\'");
	
	DROP TABLE IF EXISTS temp_line_items;
  CREATE TEMPORARY TABLE temp_line_items (id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, item_code_temp TEXT, item_name_temp TEXT, quantity_temp DOUBLE, unit_price_temp DOUBLE, uom_temp VARCHAR(255), kode_lokasi_temp DOUBLE, kode_perusahaan_temp DOUBLE, memo_line_temp TEXT );

  SET @sqlx := CONCAT('INSERT INTO temp_line_items (item_code_temp, item_name_temp, quantity_temp, unit_price_temp, uom_temp, kode_lokasi_temp, kode_perusahaan_temp, memo_line_temp) VALUES (',  "\'",sql_line_items, "\'" , ')');
  PREPARE myStmt FROM @sqlx;
	EXECUTE myStmt;
	SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
	-- SELECT unit_code INTO get_uom FROM in_01_m_item_codes WHERE item_code = 
	update temp_line_items lineitm left join in_03_m_stock_master stk on (lineitm.item_code_temp = stk.stock_code)
	set lineitm.uom_temp = stk.unit_code;
	
	IF jenis_adj = 1 THEN
		SELECT coa_positive INTO get_coa_adj FROM in_01_m_inventory_adj_types WHERE id = adj_type;
	ELSEIF jenis_adj = 2 THEN
		SELECT coa_negative INTO get_coa_adj FROM in_01_m_inventory_adj_types WHERE id = adj_type;
	ELSE
		SELECT "invalid jenis adj" AS message, 400 AS status;
		LEAVE this_procedure;
	END IF;
	SET get_trans_no = `generate_document_number`(in_kode_perusahaan, in_type_doc, in_trans_date , in_create_by);

	SELECT refs INTO get_reference FROM st_02_refs WHERE type_doc = in_type_doc AND trans_no = get_trans_no LIMIT 1;
	
	-- ambil dari price x qty line itemnya
	SELECT SUM(unit_price_temp * quantity_temp) as total_amount INTO total_amount FROM temp_line_items;
	
	
-- 		SELECT in_type_doc, get_trans_no, in_status_code, in_create_by;
	INSERT INTO in_01_t_adjustment (trans_no, type_doc, adj_type, jenis_adj, kode_lokasi, coa_adj, trans_date, total_value, status_code, kode_perusahaan, memo, create_by, create_date, reference)
  VALUES (get_trans_no, in_type_doc, adj_type, jenis_adj, in_kode_lokasi, get_coa_adj,  in_trans_date, total_amount, in_status_code, in_kode_perusahaan, in_memo, in_create_by, CURDATE(), get_reference);
	
	INSERT INTO in_02_t_adjustment_detail ( trans_no, type_doc, stock_code, item_name, quantity, unit_price, item_unit, kode_lokasi, kode_perusahaan, memo)
	SELECT get_trans_no, in_type_doc, temp.item_code_temp, temp.item_name_temp, temp.quantity_temp, temp.unit_price_temp, temp.uom_temp, in_kode_lokasi, in_kode_perusahaan, temp.memo_line_temp FROM temp_line_items temp;
	
	SET get_kode_lokasi = in_kode_lokasi;
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
	
	END; -- end this_procedure begin  
	COMMIT;
	
	SELECT get_trans_no, get_reference;
	
END