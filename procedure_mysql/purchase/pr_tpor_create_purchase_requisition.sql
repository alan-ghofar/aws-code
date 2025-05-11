CREATE DEFINER=`permen`@`%` PROCEDURE `pr_tpor_create_purchase_requisition`(in_id_menu INT, in_kode_perusahaan VARCHAR(50), in_type_doc VARCHAR(255), in_supplier_id INT, in_memo TINYTEXT, in_trans_date DATE, in_supplier_reference TINYTEXT, in_kode_lokasi VARCHAR(25), in_delivery_to_address VARCHAR(255), in_total DOUBLE, in_tax_included INT, in_tax_group_id INT, in_status_code TINYINT, in_type_beli INT, in_create_by INT, in_line_items TEXT)
BEGIN
	DECLARE get_nama_menu VARCHAR(255);
	DECLARE get_trans_no INT;
	DECLARE get_reference VARCHAR(100);
	DECLARE hasil_line_item VARCHAR(100);
	DECLARE get_total DOUBLE;
	DECLARE sql_line_items TEXT;
-- 	DECLARE EXIT HANDLER FOR SQLEXCEPTION     
-- 	 	BEGIN          
-- 		  SELECT 'failed'; -- jika ada sesuatu yg gagal maka akan muncul ini 
-- 			ROLLBACK;-- dan akan di rollback query-query sebelumnya
-- 		END;
-- 	
-- 	START TRANSACTION;
	SET @delim = "=+="; -- delimiter per row
	SET @delim2 = "=-="; -- delimiter column
	-- replace string 
	SET sql_line_items = REPLACE(in_line_items,@delim,"\'),(\'");
	SET sql_line_items = REPLACE(sql_line_items,@delim2,"\',\'");
	
	-- create table temporary (penampung) line itemnya 
	DROP TABLE IF EXISTS temp_line_items;
  CREATE TEMPORARY TABLE temp_line_items (id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, stock_code_temp VARCHAR(20), description_temp VARCHAR(200) NULL, delivery_date_temp DATE, unit_price_temp DOUBLE, uom_selected_temp VARCHAR(20), qty_input_temp DOUBLE, price_input_temp DOUBLE, item_tax_type_id_temp INT, kode_perusahaan_temp VARCHAR(50));

	-- memformat query insert ke table penampung
   SET @sqlx := CONCAT('INSERT INTO temp_line_items ( stock_code_temp, description_temp, delivery_date_temp, unit_price_temp, uom_selected_temp, qty_input_temp, price_input_temp, item_tax_type_id_temp, kode_perusahaan_temp) VALUES (',  "\'",sql_line_items, "\'" , ')');
   PREPARE myStmt FROM @sqlx;
	 EXECUTE myStmt;-- eksekusi query insert table penampung line item
	
	-- get nama menu from table menu
	SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
	
	-- get trans no and reference
	SET get_trans_no = `generate_document_number`(in_kode_perusahaan, in_type_doc, in_trans_date , in_create_by);
	SELECT refs INTO get_reference FROM st_02_refs WHERE type_doc = in_type_doc AND trans_no = get_trans_no LIMIT 1;
	
	-- insert ke table pr header
	INSERT INTO pr_01_t_purchase_requisition ( trans_no, type_doc, supplier_id, memo, trans_date, reference, supplier_reference, kode_lokasi, delivery_to_address, total, tax_included, tax_group_id, status_code, type_beli, kode_perusahaan, create_by, create_date)
VALUES ( get_trans_no, in_type_doc, in_supplier_id, in_memo, in_trans_date, get_reference, in_supplier_reference, in_kode_lokasi, in_delivery_to_address, in_total, in_tax_included, in_tax_group_id, in_status_code, in_type_beli, in_kode_perusahaan, in_create_by, CURDATE());
	
	-- insert ke table pr detail
	INSERT INTO pr_02_t_purchase_requisition_detail ( trans_no, type_doc, stock_code, `description`, delivery_date, unit_price, uom_selected, qty_input, price_input, item_tax_type_id, kode_perusahaan, create_by, create_date)
	SELECT get_trans_no, in_type_doc, temp.stock_code_temp, temp.description_temp, temp.delivery_date_temp, temp.unit_price_temp, temp.uom_selected_temp, temp.qty_input_temp, temp.price_input_temp, temp.item_tax_type_id_temp, temp.kode_perusahaan_temp, in_create_by, CURDATE() FROM temp_line_items temp;
	
	-- insert ke audit trail
	CALL add_audit_trail(in_type_doc, get_trans_no, in_create_by, in_id_menu, get_nama_menu, 'ADD', '', in_kode_perusahaan);
	  
-- 	COMMIT;
	
	SELECT get_trans_no, get_reference;
	
END