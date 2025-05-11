CREATE DEFINER=`permen`@`%` PROCEDURE `dbmodularerp`.`document_approval_transaction`(in_id_menu INT, in_kode_perusahaan VARCHAR(50), 
in_id_doc_workflow_log INT, in_type_doc VARCHAR(100), in_trans_no INT, 
in_status_action TINYINT, in_nilai INT, in_create_by INT, in_line_items TEXT, in_memo TEXT)
BEGIN
	DECLARE get_nama_menu VARCHAR(255);
	DECLARE get_keterangan VARCHAR(255);
	DECLARE get_action VARCHAR(255);
	DECLARE get_status_code_document VARCHAR(50);
	DECLARE get_urutan_tertinggi INT;
	DECLARE get_status_action_text VARCHAR(100);

	DECLARE get_type_doc VARCHAR(100);
	DECLARE get_trans_no INT;
	DECLARE get_kode_perusahaan VARCHAR(50);
	DECLARE get_kode_lokasi VARCHAR(50);
	DECLARE get_status_action TINYINT;
    DECLARE get_urutan TINYINT;
	DECLARE get_id_user INT;
	DECLARE get_id_resp INT;
	DECLARE get_type_approval TINYINT;
	DECLARE get_nilai INT;
	DECLARE valid_cek TINYINT;
	
	
	DECLARE sql_line_items TEXT;

	DECLARE EXIT HANDLER FOR SQLEXCEPTION     
	 	BEGIN          
	 	  SELECT 'failed approval document'; -- jika ada sesuatu yg gagal maka akan muncul ini 
	 		ROLLBACK;-- dan akan di rollback query-query sebelumnya
	 	END;
	
	START TRANSACTION;
	

	this_procedure:BEGIN
		set time_zone = 'Asia/Jakarta';
	
		IF in_line_items <> '' THEN
			SET @delim = "=+="; -- delimiter per row
			SET @delim2 = "=-="; -- delimiter column
			-- replace string 
			SET sql_line_items = REPLACE(in_line_items,@delim,"\'),(\'");
			SET sql_line_items = REPLACE(sql_line_items,@delim2,"\',\'");
			
			-- create table temporary (penampung) line itemnya 
			DROP TABLE IF EXISTS temp_line_items;
			CREATE TEMPORARY TABLE temp_line_items (id INT AUTO_INCREMENT PRIMARY KEY NOT NULL, 
			id_line_detail_temp INT, stock_code_temp TEXT, quantity_temp DOUBLE );
			-- memformat query insert ke table penampung
			SET @sqlx := CONCAT('INSERT INTO temp_line_items (id_line_detail_temp, stock_code_temp, quantity_temp) VALUES (',  "\'",sql_line_items, "\'" , ')');
			PREPARE myStmt FROM @sqlx;
			EXECUTE myStmt;-- eksekusi query insert table penampung line item
		END IF;
		
		-- validasi id document workflow log
		SELECT COUNT(*) INTO valid_cek FROM st_03_document_workflow_log
		WHERE delete_mark = 0 AND id = in_id_doc_workflow_log;
		IF valid_cek != 1 THEN
			SELECT "invalid id" AS message, 400 AS status;
			LEAVE this_procedure;
		END IF;
		
		
		SELECT type_doc, trans_no, status_action, urutan, id_user, id_responsibility, kode_perusahaan, kode_lokasi, type_approval, nilai INTO 
		@get_type_doc, @get_trans_no, @get_status_action, @get_urutan, @get_id_user, @get_id_resp, @get_kode_perusahaan, @get_kode_lokasi, @get_type_approval, @get_nilai
		FROM st_03_document_workflow_log 
		WHERE delete_mark = 0 
		AND id = in_id_doc_workflow_log;
	
		
		
		IF @get_trans_no IS NULL THEN
			SELECT "invalid id document workflow log" AS message, 400 AS status;
			LEAVE this_procedure;
		ELSEIF @get_trans_no != in_trans_no THEN
			SELECT "invalid trans no" AS message, 400 AS status;
			LEAVE this_procedure;
		ELSEIF @get_type_doc != in_type_doc THEN
			SELECT "invalid type doc" AS message, 400 AS status;
			LEAVE this_procedure;
		ELSEIF @get_kode_perusahaan != in_kode_perusahaan THEN
			SELECT "invalid kode_perusahaan" AS message, 400 AS status;
			LEAVE this_procedure;
		ELSEIF @get_type_approval = 1 THEN
			-- approval by id user doesnt match user id
			IF @get_id_user != in_create_by THEN
				SELECT "doesnt match user id" AS message, 400 AS status;
				LEAVE this_procedure;
			END IF;
		ELSEIF @get_type_approval = 2 THEN
			-- approval by responsibility
			SELECT COUNT(*) INTO valid_cek
			FROM sc_02_user_resp sur 
			WHERE sur.delete_mark = 0 
			AND sur.kode_perusahaan = in_kode_perusahaan
			AND sur.id_user = in_create_by
			AND sur.id_responsibility = @get_id_resp;
			
			IF valid_cek = 0 THEN
				SELECT "doesnt match responsibility" AS message, 400 AS status;
				LEAVE this_procedure;
			END IF;
		END IF;
	
		-- 0 = open , 1 = pending, 2 = approve, 3 = reject, 4 = denied, 5 = approval by pass
		IF in_status_action = 2 THEN
			SET get_action = 'APPROVE';
						
			-- cek urutan nya
			SELECT max(urutan) highlevel INTO get_urutan_tertinggi
			FROM st_03_document_workflow_log 
			WHERE delete_mark = 0 AND type_doc = in_type_doc AND trans_no = in_trans_no;
			
			IF (get_urutan_tertinggi = @get_urutan) THEN
				-- jika approval sudah di level tertinggi maka status document approve
				SET get_status_code_document = '3';
			ELSE
				-- jika belum level tertinggi maka status document tetap proses approval
				SET get_status_code_document = '2';
			END IF;
		ELSEIF in_status_action = 3 THEN
			SET get_action = 'REJECT';
			-- status document menjadi reject
			SET get_status_code_document = '-1';
			
		ELSEIF in_status_action = 4 THEN
			SET get_action = 'DENIED';
			-- status document kembali menjadi draft
			SET get_status_code_document = '1';
			
		ELSEIF in_status_action = 5 THEN
			SET get_action = 'APPROVE BY PASS';
			-- status document langsung jadi approve
			SET get_status_code_document = '3';
		ELSE 
			#invalid status action
			SET get_action = '-';
			SELECT "invalid status action" AS message, 400 AS status;
			LEAVE this_procedure;
		END IF;
		
		-- validasi document worklow log, seharusnya per nomer dokumen, hanya ada 1 yg berstatus action = 0 (open)
		SELECT COUNT(*) INTO valid_cek
		FROM st_03_document_workflow_log sdwl 
		WHERE sdwl.delete_mark =0
		AND sdwl.kode_perusahaan = in_kode_perusahaan
		AND sdwl.type_doc  = in_type_doc
		AND sdwl.trans_no  = in_trans_no
		AND sdwl.status_action  = 0;
		IF valid_cek != 1 THEN
			SELECT CONCAT("invalid status action in document workflow log") AS message, 400 status;
			LEAVE this_procedure;
		END IF;
		
		IF (in_status_action != 5) THEN
			-- validasi jika bukan approval by pass	
			IF in_status_action = 2 OR in_status_action = 3 OR in_status_action = 4 THEN
				-- cek level status (harusnya status actionnya 0(open) bukan yg lain )
				IF @get_status_action != 0 THEN
					IF @get_status_action = 1 THEN
						SELECT CONCAT("Level ini belum saatnya ", get_action)AS message, 400 AS status;
					ELSEIF @get_status_action = 2 THEN
						SELECT CONCAT("Level ini sudah berstatus approve") AS message, 400 AS status;
					ELSEIF @get_status_action = 3 THEN
						SELECT CONCAT("Level ini sudah berstatus reject") AS message, 400 AS status;
					ELSEIF @get_status_action = 4 THEN
						SELECT CONCAT("Level ini sudah berstatus denied") AS message, 400 AS status;
					ELSEIF @get_status_action = 5 THEN 
						SELECT CONCAT("Level ini sudah approval by pass") AS message, 400 AS status;
					ELSE
						SELECT CONCAT("invalid status") AS message, 400 AS status; 
					END IF;
					
					LEAVE this_procedure;
				END IF;
			ELSE
				-- kondisi jika in_status_action bukan 2,3,4,5
				SELECT CONCAT("invalid in_status_action") AS message, 400 AS status; 
			END IF;
		ELSE
			
			-- kondisi jika approval by pass, validasi current status action nya, seharusnya statusnya (0 - open / 1 - pending)
			IF @get_status_action != 0 OR @get_status_action != 1 THEN 
			
				IF @get_status_action = 2 THEN 
					SET get_status_action_text = "Approve";
				ELSEIF @get_status_action = 3 THEN
					SET get_status_action_text = "Reject";
				ELSEIF @get_status_action = 4 THEN
					SET get_status_action_text = "Denied";
				ELSEIF @get_status_action = 5 THEN
					SET get_status_action_text = "Approve by pass";
				ELSE
					SET get_status_action_text = "invalid status";
				END IF;
			
				SELECT CONCAT("Failed Approve by pass, type_doc: ", in_type_doc, 
					" trans_no: ", in_trans_no, " current status: ", get_status_action_text) AS message, 400 AS status;
				
				LEAVE this_procedure;
			END IF;
		END IF;
	
		-- validasi nilai
		IF @get_nilai IS NOT NULL THEN
			IF in_nilai > @get_nilai THEN
				SELECT CONCAT("Total Nilai melebihi level") AS message, 400 AS status;
				LEAVE this_procedure;
			END IF;
		END IF;
		
		-- get nama menu from table menu
		SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
		
		SET get_keterangan = CONCAT('status_action: ', get_action);
		
		-- update status action 
		UPDATE st_03_document_workflow_log 
		SET status_action = in_status_action, 
		approval_date = NOW(), 
		update_by = in_create_by, 
		update_date = CURDATE()
		WHERE id = in_id_doc_workflow_log;
		
		-- jika status approve (2)
		IF(in_status_action = 2) THEN
			-- update status action = 0 (open), 1 tingkat diatas urutannya
			UPDATE st_03_document_workflow_log 
			SET status_action = 0
			WHERE delete_mark = 0 AND type_doc = in_type_doc AND trans_no = in_trans_no
			AND urutan > @get_urutan ORDER BY urutan LIMIT 1;
		ELSEIF(in_status_action = 3 ) THEN
			-- update delete_mark = 1, untuk urutan diatasnya
			UPDATE st_03_document_workflow_log 
			SET delete_mark = 1, delete_by = in_create_by, delete_date = CURDATE()
			WHERE delete_mark = 0 
			AND type_doc = in_type_doc 
			AND trans_no = in_trans_no 
			AND urutan > @get_urutan;
		ELSEIF(in_status_action = 4) THEN 
			-- update status_action = 4 (denied), untuk semua urutan diatasnya
			UPDATE st_03_document_workflow_log 
			SET status_action  = 4
			WHERE delete_mark = 0 AND type_doc = in_type_doc AND trans_no = in_trans_no
			AND urutan > @get_urutan;
		ELSEIF(in_status_action = 5) THEN
			-- update status action = 2 (approve) untuk urutan dibawahnya 
			UPDATE st_03_document_workflow_log 
			SET status_action = 2
			WHERE delete_mark = 0 
			AND type_doc = in_type_doc 
			AND trans_no = in_trans_no
			AND urutan < @get_urutan;
		END IF;
		
		-- insert document approval_log
		IF in_line_items = '' THEN
			INSERT INTO st_04_document_approval_log (id_log, trans_no, type_doc, memo, create_by, create_date)
			VALUES (in_id_doc_workflow_log, in_trans_no, in_type_doc, in_memo, in_create_by, CURDATE());
		ELSE
			INSERT INTO st_04_document_approval_log (id_log, trans_no, type_doc, 
				id_det_trans , stock_code , quantity , memo, create_by, create_date)
			SELECT in_id_doc_workflow_log, in_trans_no, in_type_doc, 
				id_line_detail_temp, stock_code_temp, quantity_temp , in_memo, in_create_by, CURDATE() FROM temp_line_items;
		END IF;
	
		-- update
		CALL update_status_code_document_transaction(in_type_doc, in_trans_no, get_status_code_document, in_line_items, in_create_by);
		
		-- insert ke audit trail
		CALL add_audit_trail(in_type_doc, in_trans_no, in_create_by, in_id_menu, get_nama_menu, get_action, get_keterangan, in_kode_perusahaan);
	  	
		-- return next document status
		SELECT get_status_code_document, 200 AS status;
	  	
	END; -- end this_procedure begin
	COMMIT;
	
END