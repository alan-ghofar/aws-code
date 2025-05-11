CREATE DEFINER=`permen`@`%` PROCEDURE `dbmodularerp`.`st_document_workflow_log`( in_type_doc VARCHAR(100), in_trans_no INT, 
in_status_code VARCHAR(50), in_create_by INT, in_kode_perusahaan VARCHAR(50), in_kode_lokasi VARCHAR(50))
BEGIN
	declare jumlah_level_approval INT;
	declare jumlah_data INT;
	DECLARE EXIT HANDLER FOR SQLEXCEPTION     
		BEGIN          
		  SELECT 'failed doc work flow'; -- jika ada sesuatu yg gagal maka akan muncul ini 
			ROLLBACK;-- dan akan di rollback query-query sebelumnya
		END;
		
	START TRANSACTION;

	this_procedure:BEGIN
		SET time_zone = 'Asia/Jakarta';
		
		-- hanya status proses approval
		IF in_status_code = '2' THEN
			-- validasi keberadaan dokumen di workflow log
			SELECT COUNT(*) INTO jumlah_data 
			FROM st_03_document_workflow_log wflog 
			WHERE wflog.type_doc = in_type_doc AND wflog.trans_no = in_trans_no AND wflog.delete_mark = 0;
			IF jumlah_data THEN
				SELECT "Document approval sudah ada" AS message, 400 AS status;
				LEAVE this_procedure;
			END IF;
		
			-- cek doc workflow per lokasi
			SELECT count(*) jumlah_approval into jumlah_level_approval
			FROM st_02_document_workflow dw
			WHERE dw.delete_mark = 0
			AND dw.active = 1
			AND dw.type_doc = in_type_doc
			AND dw.kode_perusahaan = in_kode_perusahaan
			AND dw.kode_lokasi = in_kode_lokasi;
			
			IF jumlah_level_approval > 0 THEN
				-- jika ada per lokasi
				INSERT INTO st_03_document_workflow_log 
					(type_doc, trans_no, urutan, nilai, kode_perusahaan, kode_lokasi, type_approval, 
					id_user, id_responsibility, status_action, create_by, create_date, delete_mark)
				SELECT 
					in_type_doc, in_trans_no, dw.urutan, dw.nilai, in_kode_perusahaan, dw.kode_lokasi, dw.type_approval, 
					IF(dw.type_approval = 1,dw.approval_by,0) id_user, IF(dw.type_approval = 2,dw.approval_by,0) role_user, 0, 
					in_create_by, CURDATE(), 0
				FROM st_02_document_workflow dw 
				WHERE dw.delete_mark = 0 
				AND dw.active = 1 
				AND dw.type_doc = in_type_doc
				AND dw.kode_perusahaan = in_kode_perusahaan
				AND dw.kode_lokasi = in_kode_lokasi;
			END IF;
			
			IF jumlah_level_approval = 0 THEN
				-- cek doc workflow per perusahaan
				SELECT count(*) jumlah_approval into jumlah_level_approval
				FROM st_02_document_workflow dw
				WHERE dw.delete_mark = 0
				AND dw.active = 1
				AND dw.type_doc = in_type_doc
				AND dw.kode_perusahaan = in_kode_perusahaan
				AND dw.kode_lokasi IS NULL;
				
				IF jumlah_level_approval > 0 THEN
					-- jika ada per perusahaan
					INSERT INTO st_03_document_workflow_log 
						(type_doc, trans_no, urutan, nilai, kode_perusahaan, kode_lokasi, type_approval, 
						id_user, id_responsibility, status_action, create_by, create_date, delete_mark)
					SELECT 
						in_type_doc, in_trans_no, dw.urutan, dw.nilai, in_kode_perusahaan, dw.kode_lokasi, dw.type_approval, 
						IF(dw.type_approval = 1,dw.approval_by,0) id_user, IF(dw.type_approval = 2,dw.approval_by,0) role_user, 0, 
						in_create_by, CURDATE(), 0
					FROM st_02_document_workflow dw 
					WHERE dw.delete_mark = 0 
					AND dw.active = 1 
					AND dw.type_doc = in_type_doc
					AND dw.kode_perusahaan = in_kode_perusahaan
					AND dw.kode_lokasi IS NULL;
				END IF;
				
				IF jumlah_level_approval = 0 THEN 
					-- cek doc workflow  per dokumen
					SELECT count(*) jumlah_approval into jumlah_level_approval
					FROM st_02_document_workflow dw
					WHERE dw.delete_mark = 0
					AND dw.active = 1
					AND dw.type_doc = in_type_doc
					AND dw.kode_perusahaan IS NULL 
					AND dw.kode_lokasi IS NULL;
					
					IF jumlah_level_approval > 0 THEN
						-- jika ada per dokumen
						INSERT INTO st_03_document_workflow_log 
							(type_doc, trans_no, urutan, nilai, kode_perusahaan, kode_lokasi, type_approval, 
							id_user, id_responsibility, status_action, create_by, create_date, delete_mark)
						SELECT 
							in_type_doc, in_trans_no, dw.urutan, dw.nilai, in_kode_perusahaan, dw.kode_lokasi, dw.type_approval, 
							IF(dw.type_approval = 1,dw.approval_by,0) id_user, IF(dw.type_approval = 2,dw.approval_by,0) role_user, 0, 
							in_create_by, CURDATE(), 0
						FROM st_02_document_workflow dw 
						WHERE dw.delete_mark = 0 
						AND dw.active = 1 
						AND dw.type_doc = in_type_doc
						AND dw.kode_perusahaan IS NULL
						AND dw.kode_lokasi IS NULL;
					END IF;
				END IF;
			END IF;
		
			
			
			-- update status action=pending untuk urutan yg bukan pertama
			UPDATE st_03_document_workflow_log SET
				status_action = 1
			WHERE 
				type_doc = in_type_doc
				AND trans_no = in_trans_no
				AND urutan != 1;
		END IF;
	END; -- end this_procedure begin
	COMMIT;

END