CREATE DEFINER=`permen`@`%` PROCEDURE `sl_sq_edit`(`in_trans_no` INT,`in_type_doc` VARCHAR(10), `in_created_by` INT, `in_id_menu` INT, `in_kode_perusahaan` VARCHAR(10), in_keterangan VARCHAR(255), in_line_items TEXT)
BEGIN
	#Routine body goes here...
	DECLARE get_nama_menu VARCHAR(255);
	DECLARE get_keterangan VARCHAR(255);
	DECLARE get_reference VARCHAR(255);
	DECLARE get_ov_amount DOUBLE;
	DECLARE sql_line_items TEXT;
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION     
		BEGIN          
		  SELECT 400 as `status`; -- jika ada sesuatu yg gagal maka akan muncul ini 
			ROLLBACK;-- dan akan di rollback query-query sebelumnya
		END;
	
	SET time_zone = 'Asia/Jakarta';
		
	START TRANSACTION;
-- 	in_trans_no = 0 ketika transaksi baru
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
		CREATE TEMPORARY TABLE temp_line_items (id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, id_line_temp INT, quantity_temp DOUBLE, uom_temp VARCHAR(100), unit_price_temp DOUBLE, qty_input_temp DOUBLE);
		-- memformat query insert ke table penampung
		SET @sqlx := CONCAT('INSERT INTO temp_line_items (id_line_temp, quantity_temp, uom_temp, unit_price_temp, qty_input_temp) VALUES (',  "\'",sql_line_items, "\'" , ')');
		PREPARE myStmt FROM @sqlx; 
		EXECUTE myStmt;-- eksekusi query insert table penampung line item
		-- isi kolom item name dengan update join item
	-- 	UPDATE temp_line_items temp 
	-- 	LEFT JOIN in_01_m_item_codes m ON (temp.item_code_temp = m.item_code)
	-- 	SET temp.item_name_temp = m.description ;
		
-- 		get nama menu from table menu
		SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
		
-- 		get trans no and reference
		SELECT refs INTO get_reference FROM st_02_refs WHERE type_doc = in_type_doc AND trans_no = in_trans_no LIMIT 1;
		
-- 		ambil dari price x qty line itemnya
		SELECT SUM(unit_price_temp * quantity_temp) as total_amount INTO get_ov_amount FROM temp_line_items;
		
-- 		UPDATE SQ HEADER
		UPDATE sl_01_t_sales_quotation SET 
			ov_amount = get_ov_amount, 
			update_by = in_created_by, 
			update_date = CURDATE()
		WHERE 
			type_doc = in_type_doc 
			AND trans_no = in_trans_no;

-- 		UPDATE ke table customer trans
		UPDATE sl_01_t_customer_trans SET
			ov_amount = get_ov_amount, 
			update_by = in_created_by, 
			update_date = CURDATE() 
		WHERE 
			type_doc = in_type_doc 
			AND trans_no = in_trans_no;
		
-- 		UPDATE ke table sq detail item
		UPDATE sl_02_t_sales_quotation_details sq
		LEFT JOIN temp_line_items temp ON ( temp.id_line_temp = sq.id ) 
		SET 
			sq.quantity = temp.quantity_temp,
			sq.x_quantity = temp.quantity_temp,
			sq.qty_input = temp.qty_input_temp,
			sq.unit_price = temp.unit_price_temp, 
			sq.uom_input = temp.uom_temp 
		WHERE
			sq.type_doc = in_type_doc
			AND sq.trans_no = in_trans_no;
	
-- 		KETERANGAN UNTUK UPDATE SQ
		SELECT CONCAT(in_keterangan, ' #', get_reference) INTO get_keterangan;

-- 		insert ke audit trail
		CALL add_audit_trail(in_type_doc, in_trans_no, in_created_by, in_id_menu, get_nama_menu, in_keterangan, get_keterangan, in_kode_perusahaan);
		
	COMMIT;
	
	SELECT in_trans_no, get_reference;
-- 	SELECT get_ov_amount;

END