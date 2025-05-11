CREATE DEFINER=`permen`@`%` PROCEDURE `dbmodularerp`.`fi_add_gl_trans`(INOUT input_error TINYINT, OUT message_error VARCHAR(255),
OUT get_counter_this INT, OUT get_counter_balance INT,
in_trans_no int, in_type_doc varchar(100), in_trans_date date, 
in_account_code varchar(15), in_account_balance varchar(15),
in_memo tinytext, in_amount double, in_person_type_id int, in_person_id tinyblob, 
in_alloc_from_type_doc varchar(100), in_alloc_from_trans_no int, in_alloc_to_type_doc varchar(100), in_alloc_to_trans_no int,
in_src_id int, in_id_budget_mapping int, in_id_memo int, in_kode_perusahaan varchar(50),
in_cabang_code int, in_kode_lokasi varchar(50), in_create_by int, in_create_date date)
BEGIN
	
	declare get_amount_balance double;
	DECLARE EXIT HANDLER FOR SQLEXCEPTION     
		BEGIN          
			SET input_error = 1;
		  	SET message_error = 'failed add GL';-- jika ada sesuatu yg gagal maka akan muncul ini
			ROLLBACK;-- dan akan di rollback query-query sebelumnya
		END;
		
	-- START TRANSACTION;
	-- ambil nilai lawan
	set get_amount_balance = in_amount * (-1);
	INSERT INTO fi_01_t_gl_trans (type_doc, trans_no, trans_date, account, memo, amount, person_type_id, person_id, 
		alloc_from_type_doc, alloc_from_trans_no, alloc_to_type_doc, alloc_to_trans_no, 
		src_id, id_budget_mapping, id_memo, 
		kode_perusahaan, cabang_code, kode_lokasi, create_by, create_date) 
	VALUES(in_type_doc, in_trans_no, in_trans_date, in_account_code, in_memo, in_amount, in_person_type_id, in_person_id, 
		in_alloc_from_type_doc, in_alloc_from_trans_no, in_alloc_to_type_doc, in_alloc_to_trans_no,
		in_src_id, in_id_budget_mapping, in_id_memo, 
		in_kode_perusahaan, in_cabang_code, in_kode_lokasi, in_create_by, in_create_date );
	-- ambil counter gl
	set get_counter_this = LAST_INSERT_ID();

	-- simpan gl lawan
	INSERT INTO fi_01_t_gl_trans (type_doc, trans_no, trans_date, account, memo, amount, person_type_id, person_id, 
		alloc_from_type_doc, alloc_from_trans_no, alloc_to_type_doc, alloc_to_trans_no, 
		src_id, id_budget_mapping, id_memo, 
		kode_perusahaan, cabang_code, kode_lokasi, create_by, create_date, src_counter_gl) 
	VALUES(in_type_doc, in_trans_no, in_trans_date, in_account_balance, in_memo, get_amount_balance, in_person_type_id, in_person_id, 
		in_alloc_from_type_doc, in_alloc_from_trans_no, in_alloc_to_type_doc, in_alloc_to_trans_no,
		in_src_id, in_id_budget_mapping, in_id_memo, 
		in_kode_perusahaan, in_cabang_code, in_kode_lokasi, in_create_by, in_create_date, get_counter_this);
	-- ambil counter lawan
	set get_counter_balance = LAST_INSERT_ID(); 

	-- simpan src counter = counter
	UPDATE fi_01_t_gl_trans 
	SET src_counter_gl = counter 
	WHERE counter = get_counter_this;

	-- display counter 
	-- SELECT get_counter_this, get_counter_balance;
	
	-- COMMIT;

END