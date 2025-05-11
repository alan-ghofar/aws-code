CREATE DEFINER=`permen`@`%` PROCEDURE `dbmodularerp`.`in_add_stock_move`( INOUT input_error TINYINT, OUT message_error VARCHAR(255), 
	OUT get_id_stock_move INT, OUT get_counter_this INT, OUT get_counter_balance INT,
	in_trans_no INT, in_type_doc VARCHAR(100), 
	in_stock_code VARCHAR(20), in_kode_lokasi VARCHAR(50), in_trans_date DATE, in_person_id INT, in_price DOUBLE, 
	in_reference VARCHAR(100), in_qty DOUBLE, in_discount_percent DOUBLE, in_standard_cost DOUBLE, 
	in_src_id INT, in_add_fee_price DOUBLE, in_is_transit TINYINT, in_date_transit DATE, 
	in_material_manufacturing DOUBLE, in_manufacturing_cost DOUBLE, in_wo_cost DOUBLE, 
	in_kode_perusahaan VARCHAR(50), in_create_by INT, in_create_date DATE, 
	in_account_balance VARCHAR(50), in_amount_gl DOUBLE, in_person_type_id INT)
BEGIN
	DECLARE get_account_persediaan VARCHAR(15);
	DECLARE get_cabang_code INT;
	DECLARE EXIT HANDLER FOR SQLEXCEPTION     
		BEGIN          
			SET input_error = 1;
		  	SET message_error = 'failed add stock move'; -- jika ada sesuatu yg gagal maka akan muncul ini 
			ROLLBACK;-- dan akan di rollback query-query sebelumnya
		END;
		
	-- START TRANSACTION;
	this_procedure:BEGIN
		-- ambil account persediaan
		SELECT inventory_account INTO get_account_persediaan 
		FROM in_03_m_stock_master 
		WHERE stock_code = in_stock_code;
	
		-- ambil cabang dari lokasi
		SELECT cabang_code INTO get_cabang_code 
		FROM st_01_lokasi
		WHERE kode_lokasi = in_kode_lokasi;
		
		INSERT INTO in_03_t_stock_moves (trans_no, type_doc, stock_code, kode_lokasi, trans_date, 
			person_id, price, reference, qty, discount_percent, standard_cost, 
			src_id, add_fee_price, is_transit, date_transit, price_old,
			material_manufacturing, manufacturing_cost, wo_cost, 
			kode_perusahaan, create_by, create_date) 
		VALUES(in_trans_no, in_type_doc, in_stock_code, in_kode_lokasi, in_trans_date, 
			in_person_id, in_price, in_reference, in_qty, in_discount_percent, in_standard_cost, 
			in_src_id, in_add_fee_price, in_is_transit, in_date_transit, in_price, 
			in_material_manufacturing, in_manufacturing_cost, in_wo_cost, 
			in_kode_perusahaan, in_create_by, in_create_date);
		-- get id stock move
		SET get_id_stock_move = LAST_INSERT_ID();
		
		-- add gl 
		CALL fi_add_gl_trans(input_error, message_error, @get_counter_this, @get_counter_balance, in_trans_no , in_type_doc , in_trans_date, 
			get_account_persediaan, in_account_balance ,
			'memonya', in_amount_gl, in_person_type_id, in_person_id, 
			'', 0, '', 0,
			get_id_stock_move,  0, 0, in_kode_perusahaan,
			get_cabang_code, in_kode_lokasi, in_create_by, in_create_date);
		
		IF(input_error = 1) THEN
			ROLLBACK;-- rollback query-query sebelumnya
			LEAVE this_procedure;
		END IF;
			
		-- display id stock move
		-- SELECT get_id_stock_move, 200 AS status, input_error;
	END; -- end this_procedure begin
	-- COMMIT;
END