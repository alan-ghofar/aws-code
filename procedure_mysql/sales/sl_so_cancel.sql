CREATE DEFINER=`permen`@`%` PROCEDURE `dbmodularerp`.`sl_so_cancel`(`in_trans_no` INT,`in_type_doc` VARCHAR(10),`in_status_code` VARCHAR(2),`in_created_by` INT, `in_id_menu` INT, `in_kode_perusahaan` VARCHAR(10), in_keterangan VARCHAR(255), in_line_items TEXT, in_is_direct INT, in_is_cancel INT)
BEGIN	
	DECLARE get_nama_menu VARCHAR(255);
	DECLARE get_keterangan VARCHAR(255);
	DECLARE get_reference VARCHAR(255);
	DECLARE sql_line_items TEXT;
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION 
		BEGIN          
		  SELECT 400 as `status`; -- jika ada sesuatu yg gagal maka akan muncul ini 
			ROLLBACK;-- dan akan di rollback query-query sebelumnya
		END;
		
	SET time_zone = 'Asia/Jakarta';
		
-- GET NAMA MENU FROM TABEL MENU
	SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
	
-- 	GET REFERENCE
	SELECT refs INTO get_reference FROM st_02_refs WHERE trans_no = in_trans_no AND type_doc = in_type_doc AND kode_perusahaan = in_kode_perusahaan;
	
-- 	UBAH STATUS DOKUMEN
	CALL update_status_code_document_transaction(in_type_doc, in_trans_no, in_status_code, '', in_created_by);
	
-- 	UPDATE QTY SENT SQ KETIKA SO TERBUAT DARI DISPATCH SQ (IS DIRECT = 0)
	IF in_is_direct = 0 AND in_is_cancel = 1 THEN
		SET @delim = "=+="; -- delimiter per row
		SET @delim2 = "=-="; -- delimiter column
		
-- 		REPLACE STRING 
		SET sql_line_items = REPLACE(in_line_items,@delim,"\'),(\'");
		SET sql_line_items = REPLACE(sql_line_items,@delim2,"\',\'");
		
-- 		CREATE TABLE TEMPORARY (PENAMPUNG) LINE ITEM 
		DROP TABLE IF EXISTS temp_line_items_so;
		CREATE TEMPORARY TABLE temp_line_items_so (id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, src_id_temp INT, quantity_temp DOUBLE);
		
-- 		INSERT KE TABEL PENAMPUNG
		SET @sqlx := CONCAT('INSERT INTO temp_line_items_so (src_id_temp, quantity_temp) VALUES (',  "\'",sql_line_items, "\'" , ')');
		PREPARE myStmt FROM @sqlx;
		EXECUTE myStmt;-- eksekusi query insert table penampung line item
		
-- 		UPDATE QTY SENT DI SQ
		UPDATE sl_02_t_sales_quotation_details sq_line, temp_line_items_so temp
		SET sq_line.qty_sent = sq_line.qty_sent - temp.quantity_temp
		WHERE temp.src_id_temp = sq_line.id;
	END IF;

-- 	UPDATE QTY DETAIL	SO
	UPDATE sl_02_t_sales_order_details SET quantity = qty_sent WHERE trans_no = in_trans_no AND type_doc = in_type_doc;
	
-- 	UPDATE QTY DETAIL	STOCK
	UPDATE sl_02_t_sales_order_details_stock stk, sl_02_t_sales_order_details line
	SET 
		stk.quantity = line.qty_sent
	WHERE 
		stk.src_id_line = line.id
		AND line.trans_no = in_trans_no 
		AND line.type_doc = in_type_doc;
	
-- 	SET KETERANGAN UNTUK AUDIT TRAIL
	SELECT CONCAT(in_keterangan, ' #', get_reference) INTO get_keterangan;
	
	CALL add_audit_trail(in_type_doc, in_trans_no, in_created_by, in_id_menu, get_nama_menu, in_keterangan, get_keterangan, in_kode_perusahaan);
	
	COMMIT;
	
	SELECT 200 as `status`;

END