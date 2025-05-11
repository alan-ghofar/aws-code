CREATE DEFINER=`permen`@`%` PROCEDURE `sl_so_create_sales_order`(in_id_menu INT, in_kode_perusahaan VARCHAR(50), in_trans_no_sq INT, in_type_doc_from VARCHAR(100), in_type_doc_to VARCHAR(100), in_customer_code INT, in_branch_code INT, in_trans_date DATE, in_expired_date DATE, in_sales_type_id INT, in_tax_included INT, in_payment_terms_id INT, in_ship_via INT, in_deliver_to TINYTEXT, in_delivery_address TINYTEXT, in_contact_phone VARCHAR(30), in_customer_ref VARCHAR(255), in_memo TINYTEXT, in_tax_group_id INT, in_ov_freight DOUBLE,  in_ov_discount DOUBLE, in_kode_lokasi VARCHAR(50), in_salesman_id INT, in_routes_id INT, in_status_code VARCHAR(50), in_is_android_toko INT, in_is_direct INT, in_create_by INT, in_trans_no INT, in_line_items TEXT, in_line_item_combo TEXT)
BEGIN

-- 	FUNCTION LAMBDA: DISPATCH_SQ_TO_SO, SALES_ORDER

	DECLARE get_nama_menu VARCHAR(255);
	DECLARE get_trans_no INT;
	DECLARE get_reference VARCHAR(100);
	DECLARE hasil_line_item VARCHAR(100);
	DECLARE get_ov_amount DOUBLE;
	DECLARE get_ov_gst DOUBLE;
	DECLARE get_ov_gst_freight DOUBLE;
	DECLARE sql_line_items TEXT;
	DECLARE sql_detail_stock TEXT;


	DECLARE EXIT HANDLER FOR SQLEXCEPTION     
		BEGIN          
		  SELECT 'failed'; -- jika ada sesuatu yg gagal maka akan muncul ini 
			ROLLBACK;-- dan akan di rollback query-query sebelumnya
		END;
		
	-- DECLARE EXIT HANDLER FOR SQLWARNING     BEGIN          ROLLBACK;    END;
	
	START TRANSACTION;
	
-- 	GET NAMA MENU FROM TABEL MENU
	SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
		
	IF in_trans_no = 0 THEN
		SET @delim = "=+="; -- delimiter per row
		SET @delim2 = "=-="; -- delimiter column
		-- replace string 
		SET sql_line_items = REPLACE(in_line_items,@delim,"\'),(\'");
		SET sql_line_items = REPLACE(sql_line_items,@delim2,"\',\'");
		
		SET sql_detail_stock = REPLACE(in_line_item_combo,@delim,"\'),(\'");
		SET sql_detail_stock = REPLACE(sql_detail_stock,@delim2,"\',\'");
		/**
		intinya string tersebut dirubah formatnya dari
		item_code=-=qty=-=price=+=item_code=-=qty=-=price=+=item_code=-=qty=-=price dst..
		contoh: 101=-=1=-=10000=+=102=-=3=-=20000=+=103=-=10=-=5000
		
		menjadi seperti berikut:
		('item_code','qty','price'),('item_code','qty','price'),('item_code','qty','price') dst....
		contoh: ('101','1','10000'),('102','3','20000'),('103','10','5000')
		*/
		
		-- create table temporary (penampung) line itemnya 
		DROP TABLE IF EXISTS temp_line_items_so;
		CREATE TEMPORARY TABLE temp_line_items_so (id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, src_id_temp INT, item_code_temp TEXT, item_name_temp TEXT, quantity_temp DOUBLE, unit_price_temp DOUBLE, uom_selected_temp VARCHAR(20), quantity_input_temp DOUBLE, price_input_temp DOUBLE, kode_perusahaan_temp VARCHAR(50), tax_type_item_id_temp INT, memo_line_temp TEXT );
		
		-- memformat query insert ke table penampung
		SET @sqlx := CONCAT('INSERT INTO temp_line_items_so (src_id_temp, item_code_temp, item_name_temp, quantity_temp, unit_price_temp, uom_selected_temp, quantity_input_temp, price_input_temp, kode_perusahaan_temp, tax_type_item_id_temp, memo_line_temp) VALUES (',  "\'",sql_line_items, "\'" , ')');
		PREPARE myStmt FROM @sqlx;
		EXECUTE myStmt;-- eksekusi query insert table penampung line item
		
		
-- 		TABEL TEMPORARY UNTUK DETAIL STOCK
		DROP TABLE IF EXISTS temp_detail_stock_so;
		CREATE TEMPORARY TABLE temp_detail_stock_so (id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, src_id_temp INT, item_code_temp TEXT, stock_code_temp TEXT, item_name_temp TEXT, quantity_temp DOUBLE, unit_price_temp DOUBLE, uom_selected_temp VARCHAR(20), quantity_input_temp DOUBLE, price_input_temp DOUBLE, kode_perusahaan_temp VARCHAR(50), tax_type_item_id_temp INT, memo_line_temp TEXT );
		
-- 		memformat query insert ke table detail stock penampung
		SET @sqlDetail := CONCAT('INSERT INTO temp_detail_stock_so (src_id_temp, item_code_temp, stock_code_temp, item_name_temp, quantity_temp, unit_price_temp, uom_selected_temp, quantity_input_temp, price_input_temp, kode_perusahaan_temp, tax_type_item_id_temp, memo_line_temp) VALUES (',  "\'",sql_detail_stock, "\'" , ')');
		PREPARE myStmtDetail FROM @sqlDetail;
		EXECUTE myStmtDetail;-- eksekusi query insert table penampung detail stock
		
		
-- 		GET TRANS_NO DAN REFERENCE
		SET get_trans_no = `generate_document_number`(in_kode_perusahaan, in_type_doc_to, in_trans_date, in_create_by);
		SELECT refs INTO get_reference FROM st_02_refs WHERE type_doc = in_type_doc_to AND trans_no = get_trans_no LIMIT 1;
		
		
-- 		GET TOTAL AMOUNT DARI PRICE*QTY LINE ITEM
		SELECT SUM(unit_price_temp * quantity_temp) as total_amount INTO get_ov_amount FROM temp_line_items_so;
		SET get_ov_gst = 0;
		SET get_ov_gst_freight = 0;
		
		
-- 		INSERT KE SO HEADER
		INSERT INTO sl_01_t_sales_order (trans_no, type_doc, customer_code, branch_code, reference, trans_date, expired_date, sales_type_id, tax_included, payment_terms_id, ship_via, deliver_to, delivery_address, contact_phone, customer_ref, memo, kode_lokasi, tax_group_id, ov_amount, ov_gst, ov_freight, ov_gst_freight, salesman_id, routes_id, status_code, from_android_toko, is_direct, kode_perusahaan, create_by, create_date)
		VALUES (get_trans_no, in_type_doc_to, in_customer_code, in_branch_code, get_reference,  in_trans_date, in_expired_date, in_sales_type_id, in_tax_included, in_payment_terms_id, in_ship_via, in_deliver_to, in_delivery_address, in_contact_phone, in_customer_ref, in_memo, in_kode_lokasi, in_tax_group_id, get_ov_amount, get_ov_gst, in_ov_freight, get_ov_gst_freight, in_salesman_id, in_routes_id, in_status_code, in_is_android_toko, in_is_direct, in_kode_perusahaan, in_create_by, CURDATE());


-- 		INSERT KE TABLE CUSTOMER TRANS
		CALL sl_add_t_customer_trans(get_trans_no, in_type_doc_to, get_reference, in_customer_code, in_customer_ref, in_branch_code, in_trans_date, get_ov_amount, get_ov_gst, in_ov_freight, get_ov_gst_freight, in_ov_discount, in_kode_lokasi, in_salesman_id, in_status_code, in_kode_perusahaan, in_create_by);
		
		
-- 		INSERT KE TABLE SO DETAIL
		INSERT INTO sl_02_t_sales_order_details ( trans_no, type_doc, item_code, item_name, quantity, x_quantity, qty_input, unit_price, price_input, uom_input, memo, kode_perusahaan, src_trans_no, src_id_line, src_type_doc, item_tax_type_id)
		SELECT get_trans_no, in_type_doc_to, temp.item_code_temp, temp.item_name_temp, temp.quantity_temp, temp.quantity_temp as qtyx, temp.quantity_input_temp as qtyin, temp.unit_price_temp, temp.price_input_temp as pricein, temp.uom_selected_temp, temp.memo_line_temp, temp.kode_perusahaan_temp, in_trans_no_sq, temp.src_id_temp, in_type_doc_from, temp.tax_type_item_id_temp FROM temp_line_items_so temp;
		
		
-- 		INSERT KE TABEL SO DETAIL STOCK
		INSERT INTO sl_02_t_sales_order_details_stock 
			( trans_no, type_doc, stock_code, stock_desc, quantity, x_quantity, unit_price, kode_lokasi, src_id_line, kode_perusahaan, item_tax_type_id )
		SELECT
			get_trans_no, in_type_doc_to, temp.stock_code_temp, temp.item_name_temp, temp.quantity_temp, temp.quantity_temp, temp.unit_price_temp, in_kode_lokasi, line.id, in_kode_perusahaan, tax_type_item_id_temp
		FROM 
			temp_detail_stock_so temp
			LEFT JOIN sl_02_t_sales_order_details line ON (temp.item_code_temp = line.item_code)
		WHERE
			line.type_doc = in_type_doc_to 
			AND line.trans_no = get_trans_no
			AND line.kode_perusahaan = in_kode_perusahaan;
		
		
		IF (in_trans_no_sq > 0) THEN
	-- 	UPDATE QTY SEND DI SALES QUOTATION
			UPDATE sl_02_t_sales_quotation_details sq_line, temp_line_items_so temp
			SET sq_line.qty_sent = sq_line.qty_sent + temp.quantity_temp
			WHERE temp.src_id_temp = sq_line.id AND sq_line.type_doc = in_type_doc_from;
		END IF;
	ELSE
		SET get_trans_no = in_trans_no;
		SELECT refs INTO get_reference FROM st_02_refs WHERE type_doc = in_type_doc_to AND trans_no = get_trans_no LIMIT 1;
		
		
		CALL update_status_code_document_transaction(in_type_doc_to, get_trans_no, in_status_code, '', in_create_by);
		
	END IF;
	
	
-- 	INSERT KE TABLE DOCUMENT WORK FLOW LOG
	CALL st_document_workflow_log(in_type_doc_to, get_trans_no, in_status_code, in_create_by, in_kode_perusahaan, in_kode_lokasi);
	
	
-- 	INSERT AUDIT TRAIL
	CALL add_audit_trail(in_type_doc_to, get_trans_no, in_create_by, in_id_menu, get_nama_menu, 'ADD', '', in_kode_perusahaan);
	  
	COMMIT;
	
	SELECT get_trans_no, get_reference;
	
END