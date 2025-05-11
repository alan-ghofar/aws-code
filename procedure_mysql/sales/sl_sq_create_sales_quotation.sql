CREATE DEFINER=`permen`@`%` PROCEDURE `dbmodularerp`.`sl_sq_create_sales_quotation`(in_id_menu INT, in_kode_perusahaan VARCHAR(50), in_type_doc VARCHAR(100), in_customer_code INT, in_branch_code INT, in_trans_date DATE, in_expired_date DATE, in_sales_type_id INT, in_tax_included INT, in_payment_terms_id INT, in_ship_via INT, in_deliver_to TINYTEXT, in_delivery_address TINYTEXT, in_contact_phone VARCHAR(30), in_customer_ref VARCHAR(255), in_memo TINYTEXT, in_tax_group_id INT, in_ov_freight DOUBLE,  in_ov_discount DOUBLE,in_kode_lokasi VARCHAR(50),  in_salesman_id INT, in_routes_id INT, in_status_code VARCHAR(50), in_create_by INT, in_trans_no INT,  in_line_items TEXT)
BEGIN
	DECLARE get_nama_menu VARCHAR(255);
	DECLARE get_trans_no INT;
	DECLARE get_reference VARCHAR(100);
	DECLARE hasil_line_item VARCHAR(100);
	DECLARE get_ov_amount DOUBLE;
	DECLARE get_ov_gst DOUBLE;
	DECLARE get_ov_gst_freight DOUBLE;
	DECLARE sql_line_items TEXT;
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION     
		BEGIN          
		  SELECT 'failed'; -- jika ada sesuatu yg gagal maka akan muncul ini 
			ROLLBACK;-- dan akan di rollback query-query sebelumnya
		END;
		
	-- DECLARE EXIT HANDLER FOR SQLWARNING     BEGIN          ROLLBACK;    END;
	
	START TRANSACTION;
-- 	in_trans_no = 0 ketika transaksi baru
	IF (in_trans_no = 0) THEN
		SET @delim = "=+="; -- delimiter per row
		SET @delim2 = "=-="; -- delimiter column
		-- replace string 
		SET sql_line_items = REPLACE(in_line_items,@delim,"\'),(\'");
		SET sql_line_items = REPLACE(sql_line_items,@delim2,"\',\'");
		/**
		intinya string tersebut dirubah formatnya dari
		item_code=-=qty=-=price=+=item_code=-=qty=-=price=+=item_code=-=qty=-=price dst..
		contoh: 101=-=1=-=10000=+=102=-=3=-=20000=+=103=-=10=-=5000
		
		menjadi seperti berikut:
		('item_code','qty','price'),('item_code','qty','price'),('item_code','qty','price') dst....
		contoh: ('101','1','10000'),('102','3','20000'),('103','10','5000')
		*/
		
		-- create table temporary (penampung) line itemnya 
		DROP TABLE IF EXISTS temp_line_items;
		CREATE TEMPORARY TABLE temp_line_items (id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, item_code_temp TEXT, item_name_temp TEXT, quantity_temp DOUBLE, unit_price_temp DOUBLE, uom_selected_temp VARCHAR(20), quantity_input_temp DOUBLE, price_input_temp DOUBLE, kode_perusahaan_temp VARCHAR(50), tax_type_item_id_temp INT, memo_line_temp TEXT );
		-- memformat query insert ke table penampung
		SET @sqlx := CONCAT('INSERT INTO temp_line_items (item_code_temp, item_name_temp, quantity_temp, unit_price_temp, uom_selected_temp, quantity_input_temp, price_input_temp, kode_perusahaan_temp, tax_type_item_id_temp, memo_line_temp) VALUES (',  "\'",sql_line_items, "\'" , ')');
		PREPARE myStmt FROM @sqlx;
		EXECUTE myStmt;-- eksekusi query insert table penampung line item
		-- isi kolom item name dengan update join item
	-- 	UPDATE temp_line_items temp 
	-- 	LEFT JOIN in_01_m_item_codes m ON (temp.item_code_temp = m.item_code)
	-- 	SET temp.item_name_temp = m.description ;
		
		-- get nama menu from table menu
		SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
		
		-- get trans no and reference
		SET get_trans_no = `generate_document_number`(in_kode_perusahaan, in_type_doc, in_trans_date , in_create_by);
		SELECT refs INTO get_reference FROM st_02_refs WHERE type_doc = in_type_doc AND trans_no = get_trans_no LIMIT 1;
		
		-- ambil dari price x qty line itemnya
		SELECT SUM(unit_price_temp * quantity_temp) as total_amount INTO get_ov_amount FROM temp_line_items;
		SET get_ov_gst = 0;
		SET get_ov_gst_freight = 0;
		
		
		-- insert ke table sq header
		INSERT INTO sl_01_t_sales_quotation (trans_no, type_doc, customer_code, branch_code, reference, trans_date, expired_date, sales_type_id, tax_included, payment_terms_id, ship_via, deliver_to, delivery_address, contact_phone, customer_ref, memo, kode_lokasi, tax_group_id, ov_amount, ov_gst, ov_freight, ov_gst_freight, salesman_id, routes_id, status_code, kode_perusahaan, create_by, create_date)
		VALUES (get_trans_no, in_type_doc, in_customer_code, in_branch_code, get_reference,  in_trans_date, in_expired_date, in_sales_type_id, in_tax_included, in_payment_terms_id, in_ship_via, in_deliver_to, in_delivery_address, in_contact_phone, in_customer_ref, in_memo, in_kode_lokasi, in_tax_group_id, get_ov_amount, get_ov_gst, in_ov_freight, get_ov_gst_freight, in_salesman_id, in_routes_id, in_status_code, in_kode_perusahaan, in_create_by, CURDATE());

		-- insert ke table customer trans
		CALL sl_add_t_customer_trans(get_trans_no, in_type_doc, get_reference, in_customer_code, in_customer_ref , in_branch_code, in_trans_date, get_ov_amount, get_ov_gst, in_ov_freight, get_ov_gst_freight, in_ov_discount, in_kode_lokasi, in_salesman_id, in_status_code, in_kode_perusahaan, in_create_by);
		
		-- insert ke table sq detail item
		INSERT INTO sl_02_t_sales_quotation_details ( trans_no, type_doc, item_code, item_name, quantity, x_quantity, qty_input, unit_price, price_input, uom_input, memo, kode_perusahaan, item_tax_type_id)
		SELECT get_trans_no, in_type_doc, temp.item_code_temp, temp.item_name_temp, temp.quantity_temp, temp.quantity_temp as qtyx, temp.quantity_input_temp as qtyin, temp.unit_price_temp, temp.price_input_temp as pricein, temp.uom_selected_temp, temp.memo_line_temp, temp.kode_perusahaan_temp, temp.tax_type_item_id_temp FROM temp_line_items temp;
	
	ELSE
		SET get_trans_no = in_trans_no;
		SELECT refs INTO get_reference FROM st_02_refs WHERE type_doc = in_type_doc AND trans_no = get_trans_no LIMIT 1;
		
		-- get nama menu from table menu
		SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
		
		CALL update_status_code_document_transaction(in_type_doc, get_trans_no, in_status_code, '', in_create_by);
		
	END IF;
	
	-- insert ke table document workflow log
	CALL st_document_workflow_log(in_type_doc, get_trans_no, in_status_code, in_create_by, in_kode_perusahaan, in_kode_lokasi);
	
-- 	IF in_status_code = 2 THEN
-- 		INSERT INTO st_03_document_workflow_log (type_doc, trans_no, urutan, nilai, kode_perusahaan, kode_lokasi, type_approval, status_action, delete_mark)
-- 		SELECT in_type_doc, get_trans_no, dw.urutan, dw.nilai, dw.kode_perusahaan, dw.kode_lokasi, dw.type_approval, 0, 0
-- 		FROM st_02_document_workflow dw WHERE dw.delete_mark = 0 AND dw.active = 1;
-- 	END IF;
	
	
	-- insert ke audit trail
	CALL add_audit_trail(in_type_doc, get_trans_no, in_create_by, in_id_menu, get_nama_menu, 'ADD', '', in_kode_perusahaan);
	  
	COMMIT;
	
	SELECT get_trans_no, get_reference;
	
END