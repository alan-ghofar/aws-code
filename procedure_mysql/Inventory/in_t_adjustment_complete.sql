CREATE DEFINER=`permen`@`%` PROCEDURE `dbmodularerp`.`in_t_adjustment_complete`(in_id_menu INT, in_type_doc VARCHAR(100), in_trans_no INT, in_create_by INT)
BEGIN
	DECLARE get_nama_menu VARCHAR(255);
	DECLARE get_keterangan VARCHAR(255);
	DECLARE get_action VARCHAR(255);

	DECLARE get_reference VARCHAR(100);
	DECLARE get_adj_type INT;
	DECLARE get_jenis_adj TINYINT;
	DECLARE get_coa_adj VARCHAR(15);
	DECLARE get_kode_lokasi VARCHAR(50);
	DECLARE get_trans_date DATE;
	DECLARE get_total_value DOUBLE;
	DECLARE get_status_code_document VARCHAR(50);
	DECLARE get_kode_perusahaan VARCHAR(50);
	DECLARE get_memo TEXT;

	DECLARE get_line_id INT;
	DECLARE get_line_stock_code VARCHAR(20);
	DECLARE get_line_item_name TINYTEXT;
	DECLARE get_line_quantity DOUBLE;
	DECLARE get_line_unit_price DOUBLE;
	DECLARE get_line_kode_lokasi VARCHAR(50);
	DECLARE get_line_kode_perusahaan VARCHAR(50);
	DECLARE get_line_standard_cost DOUBLE;
	DECLARE get_line_amount_gl DOUBLE;

	DECLARE status_complete VARCHAR(50);
	DECLARE valid_cek TINYINT;
	DECLARE input_error TINYINT;
	DECLARE message_error VARCHAR(255);
	DECLARE done INT DEFAULT FALSE;
	DECLARE
		loop_line_detail_cursor CURSOR FOR 
		SELECT 
		itad.id, 
		itad.stock_code,
		itad.item_name,
		itad.quantity,
		itad.unit_price,
		itad.kode_lokasi,
		itad.kode_perusahaan
		FROM in_02_t_adjustment_detail itad  
		WHERE itad.type_doc = in_type_doc
		AND itad.trans_no  = in_trans_no; -- get data line detail
		
	DECLARE CONTINUE HANDLER FOR NOT FOUND
		SET done = TRUE;
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION     
	 	BEGIN          
	 	  SELECT 'failed, complete adjustment'; -- jika ada sesuatu yg gagal maka akan muncul ini 
	 		ROLLBACK;-- dan akan di rollback query-query sebelumnya
	 	END;
	
	START TRANSACTION;

	this_procedure:BEGIN
		SET @input_error = 0;
		SET time_zone = 'Asia/Jakarta';
		SET status_complete = '6';-- cek document status
		SET get_action = 'COMPLETE';
		SET get_keterangan = CONCAT('status_action: ', get_action);
		-- get nama menu from table menu
		SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
		-- get data transaksi header
		SELECT 
			reference, adj_type, jenis_adj, coa_adj, kode_lokasi, 
			trans_date, total_value, status_code, kode_perusahaan, memo
  		INTO 
  			get_reference, get_adj_type, get_jenis_adj, get_coa_adj, get_kode_lokasi, 
  			get_trans_date, get_total_value, get_status_code_document, get_kode_perusahaan, get_memo
  		FROM in_01_t_adjustment
		WHERE type_doc = in_type_doc AND trans_no = in_trans_no;
	
		-- validasi stock hanya untuk adj negative
		IF get_jenis_adj = 2 THEN
			-- create table temporary to check qoh is safe?
			DROP TABLE IF EXISTS temp_line_items;
	  		CREATE TEMPORARY TABLE temp_check_qoh (id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, stock_code_temp VARCHAR(20), quantity_temp DOUBLE, 
	  			kode_lokasi_temp VARCHAR(50), kode_perusahaan_temp VARCHAR(50), trans_date_temp DATE );
			-- fill the temprorary table from detail
	  		-- untuk stock code sama akan ditotal 
	  		INSERT INTO temp_check_qoh (stock_code_temp, quantity_temp, kode_lokasi_temp, kode_perusahaan_temp, trans_date_temp) 
			SELECT itad.stock_code, SUM(itad.quantity) as total_qty, itad.kode_lokasi, itad.kode_perusahaan
			FROM in_02_t_adjustment_detail itad  
			WHERE itad.type_doc = in_type_doc AND itad.trans_no  = in_trans_no
			GROUP BY itad.stock_code ;
			
			-- cek validasi qoh
			SELECT dbmodular.in_check_qoh_available() INTO valid_cek;
			IF valid_cek = 0 THEN
				SELECT "stock quantity on hand is not available" AS message, 400 AS status;
				LEAVE this_procedure;
			END IF;
		END IF;
	
		-- validasi status document seharusnya 3 (approved) meskipun tanpa approval
		IF get_status_code_document = status_complete THEN
			SELECT "This Document has been completed" AS message, 400 AS status;
			LEAVE this_procedure;
		ELSEIF get_status_code_document != 3 THEN
			SELECT "Invalid Status Document" AS message, 400 AS status;
			LEAVE this_procedure;
		END IF;
		
		-- loop
		OPEN loop_line_detail_cursor;
		line_loop:LOOP
	    FETCH loop_line_detail_cursor INTO 
		    get_line_id, 
			get_line_stock_code,
			get_line_item_name,
			get_line_quantity,
			get_line_unit_price,
			get_line_kode_lokasi,
			get_line_kode_perusahaan;
		
			IF done THEN 
				LEAVE line_loop;
			END IF;
		
			-- ambil nilai standard cost
			SET get_line_standard_cost = get_line_unit_price;
		
			-- set nilai amount gl
			IF get_jenis_adj = 1 THEN 
				SET get_line_amount_gl = get_line_unit_price * get_line_quantity;
			ELSEIF get_jenis_adj = 2 THEN -- negative 
				SET get_line_amount_gl = get_line_standard_cost * get_line_quantity;
			ELSE
				SELECT "invalid jenis adj" as message, 400 as status;
				LEAVE this_procedure;
			END IF;
			
			-- add stock move beserta glnya
			CALL in_add_stock_move(@input_error, @message_error, @get_id_stock_move, @get_counter_this, @get_counter_balance, 
				in_trans_no, in_type_doc, 
				get_line_stock_code, get_kode_lokasi, get_trans_date, get_adj_type, get_line_unit_price, 
				get_reference, get_line_quantity, 0, get_line_standard_cost, 
				get_line_id, 0, 0, '0000-00-00', 0, 0, 0, 
				get_kode_perusahaan, in_create_by, CURDATE(), 
				get_coa_adj, get_line_amount_gl, 0);
			
			IF @input_error = 1 THEN
				SELECT CONCAT("failed complete this transaction, ", @message_error) AS message, 400 AS status; 
				LEAVE this_procedure;
			END IF;
	
		END LOOP line_loop;
		CLOSE loop_line_detail_cursor;
	
		-- update status code
		CALL update_status_code_document_transaction(in_type_doc, in_trans_no, status_complete, '', in_create_by);
		
		-- insert ke audit trail
		CALL add_audit_trail(in_type_doc, in_trans_no, in_create_by, in_id_menu, get_nama_menu, get_action, get_keterangan, get_kode_perusahaan);
		
		-- display sukses
		SELECT "Adjustment Complete Successful" AS message, 200 AS status;
	END; -- end this_procedure begin
	COMMIT;
END