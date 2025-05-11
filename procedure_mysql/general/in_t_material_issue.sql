CREATE DEFINER=`permen`@`%` PROCEDURE `in_t_material_issue`(in_id_menu INT, in_kode_perusahaan VARCHAR(50), in_type_doc VARCHAR(100), in_mat_issue_type INT, in_kode_lokasi VARCHAR(5), in_trans_date DATE, in_memo TEXT, in_status_code VARCHAR(50), in_create_by INT, in_line_items TEXT)
BEGIN
	DECLARE get_nama_menu VARCHAR(255);
	DECLARE get_trans_no INT;
	DECLARE get_reference VARCHAR(100);
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
	SET @delim = "=+=";
	SET @delim2 = "=-=";
	
	SET sql_line_items = REPLACE(in_line_items,@delim,"\'),(\'");
	SET sql_line_items = REPLACE(sql_line_items,@delim2,"\',\'");
	
	DROP TABLE IF EXISTS temp_line_items;
  CREATE TEMPORARY TABLE temp_line_items (id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, stock_code_temp TEXT, item_name_temp TEXT, quantity_temp DOUBLE, unit_price_temp DOUBLE, uom_temp VARCHAR(255), kode_lokasi_temp DOUBLE, kode_perusahaan_temp DOUBLE, memo_line_temp TEXT );
	
  SET @sqlx := CONCAT('INSERT INTO temp_line_items (stock_code_temp, item_name_temp, quantity_temp, unit_price_temp, uom_temp, kode_lokasi_temp, kode_perusahaan_temp, memo_line_temp) VALUES (',  "\'",sql_line_items, "\'" , ')');
  PREPARE myStmt FROM @sqlx;
	EXECUTE myStmt;
	
	SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
	
	SELECT coa INTO get_coa_adj FROM in_01_m_material_issue_types WHERE id = in_mat_issue_type;
	
	SET get_trans_no = `generate_document_number`(in_kode_perusahaan, in_type_doc, in_trans_date , in_create_by);
	SELECT refs INTO get_reference FROM st_02_refs WHERE type_doc = in_type_doc AND trans_no = get_trans_no LIMIT 1;
	
	-- ambil dari price x qty line itemnya
	SELECT SUM(unit_price_temp * quantity_temp) as total_amount INTO total_amount FROM temp_line_items;
	
	
	INSERT INTO in_01_t_material_issue (trans_no, type_doc, mat_issue_type, kode_lokasi, coa_adj, trans_date, total_value, status_code, kode_perusahaan, memo, create_by, create_date, reference)
  VALUES (get_trans_no, in_type_doc, in_mat_issue_type, in_kode_lokasi, get_coa_adj,  in_trans_date, total_amount, in_status_code, in_kode_perusahaan, in_memo, in_create_by, CURDATE(), get_reference);
	
	INSERT INTO in_02_t_material_issue_detail ( trans_no, type_doc, stock_code, item_name, quantity, unit_price, item_unit, kode_lokasi, kode_perusahaan, memo)
	SELECT get_trans_no, in_type_doc, temp.stock_code_temp, temp.item_name_temp, temp.quantity_temp, temp.unit_price_temp, temp.uom_temp, in_kode_lokasi, in_kode_perusahaan, temp.memo_line_temp FROM temp_line_items temp;
	
	
	-- insert ke table document workflow jika status_code = 2
	CALL st_document_workflow_log(in_type_doc, get_trans_no, in_status_code, in_create_by, in_kode_perusahaan, in_kode_lokasi);
	
	
	-- insert ke audit trail
	CALL add_audit_trail(in_type_doc, get_trans_no, in_create_by, in_id_menu, get_nama_menu, 'ADD', '', in_kode_perusahaan);
	  
	COMMIT;
	
	SELECT get_trans_no, get_reference;
	
END