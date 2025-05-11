CREATE DEFINER=`permen`@`%` PROCEDURE `in_m_item_combo`(in_id_menu INT, `in_item_code` VARCHAR(20), `in_stock_code` VARCHAR(20), `in_description` VARCHAR(200), `in_category_code` SMALLINT, `in_quantity` DOUBLE, `in_is_foreign` TINYINT, `in_unit_code` VARCHAR(20), `in_berat` DOUBLE, `in_panjang` DOUBLE, `in_lebar` DOUBLE, `in_tinggi` DOUBLE, `in_biaya` DOUBLE, `in_promo_active` TINYINT, `in_koefisien` DOUBLE, `in_is_combo` TINYINT, `in_kode_perusahaan` VARCHAR(50), `in_active` INT, `in_create_by` INT, in_line_items TEXT)
BEGIN
	DECLARE get_nama_menu VARCHAR(255);
	DECLARE get_last_id_insert INT;
	DECLARE hasil_line_item VARCHAR(200);
	DECLARE sql_line_items TEXT;
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN
		  SELECT 'failed'; -- jika ada sesuatu yg gagal maka akan muncul ini 
			ROLLBACK;-- dan akan di rollback query-query sebelumnya
		END;
		
	START TRANSACTION;

	SET @delim = "=+="; -- delimiter per row
	SET @delim2 = "=-="; -- delimiter column
	
	-- replace string 
	SET sql_line_items = REPLACE(in_line_items,@delim,"\'),(\'");
	SET sql_line_items = REPLACE(sql_line_items,@delim2,"\',\'");
	
	-- create table temporary (penampung) line itemnya 
	DROP TABLE IF EXISTS temp_line_items;
  CREATE TEMPORARY TABLE temp_line_items (id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, line_item_code_temp TEXT, line_stock_code_temp TEXT, quantity_temp DOUBLE, koef_temp DOUBLE, kode_perusahaan_temp VARCHAR(50), active_temp INT(1));
	
	-- memformat query insert ke table penampung
  SET @sqlx := CONCAT('INSERT INTO temp_line_items (line_item_code_temp, line_stock_code_temp, quantity_temp, koef_temp, kode_perusahaan_temp, active_temp) VALUES (',  "\'",sql_line_items, "\'" , ')');
  PREPARE myStmt FROM @sqlx;
	EXECUTE myStmt;
	
	
	-- insert ke table in_01_m_item_codes
	INSERT INTO in_01_m_item_codes (item_code, stock_code, description, category_code, quantity, is_foreign, unit_code, berat, panjang, lebar, tinggi, biaya, promo_actived, koefisien, is_combo, kode_perusahaan, active, create_by, create_date, delete_mark) VALUES (in_item_code, in_stock_code, in_description, in_category_code, in_quantity,  in_is_foreign, in_unit_code, in_berat, in_panjang, in_lebar, in_tinggi, in_biaya, in_promo_active, in_koefisien, in_is_combo, in_kode_perusahaan, in_active, in_create_by, CURDATE(), 0);
	
	SET get_last_id_insert = LAST_INSERT_ID();

	-- insert ke table in_04_m_item_combo
	INSERT INTO in_04_m_item_combo ( item_code, stock_code, quantity, koefisien_harga, kode_perusahaan, active)
	SELECT temp.line_item_code_temp, temp.line_stock_code_temp, temp.quantity_temp, temp.koef_temp, temp.kode_perusahaan_temp, temp.active_temp FROM temp_line_items temp;
	
	
	-- update in_04_m_item_combo created by created date
	UPDATE in_04_m_item_combo SET create_by = in_create_by, create_date = CURDATE(), active = in_active WHERE item_code = in_item_code;
	
	SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
	CALL add_audit_master(in_id_menu, get_nama_menu, 'create', 'Tambah item combo', in_create_by, get_last_id_insert, 'Item Combo', 'in_01_m_item_codes', in_kode_perusahaan);
	
	COMMIT;
	
	SELECT get_last_id_insert;


END