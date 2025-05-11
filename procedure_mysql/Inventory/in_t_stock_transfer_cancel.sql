CREATE DEFINER=`permen`@`%` PROCEDURE `in_t_stock_transfer_cancel`(`in_trans_no` INT,`in_type_doc` VARCHAR(10),`in_status_code` VARCHAR(2),`in_created_by` INT, `in_id_menu` INT, `in_kode_perusahaan` VARCHAR(10), in_keterangan VARCHAR(255))
BEGIN	
	DECLARE get_nama_menu VARCHAR(255);
	DECLARE get_keterangan VARCHAR(255);
	DECLARE get_reference VARCHAR(255);
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION     
		BEGIN          
		  SELECT 400 as `status`; -- jika ada sesuatu yg gagal maka akan muncul ini 
			ROLLBACK;-- dan akan di rollback query-query sebelumnya
		END;
		
		
-- GET NAMA MENU FROM TABEL MENU
	SELECT nama_menu INTO get_nama_menu FROM sc_02_menu WHERE id = in_id_menu;
	
-- 	GET REFERENCE
	SELECT refs INTO get_reference FROM st_02_refs WHERE trans_no = in_trans_no AND type_doc = in_type_doc AND kode_perusahaan = in_kode_perusahaan;
	
-- 	UBAH STATUS DOKUMEN
	CALL update_status_code_document_transaction(in_type_doc, in_trans_no, in_status_code, '', in_created_by);
	
-- 	SET KETERANGAN UNTUK AUDIT TRAIL
	SELECT CONCAT(in_keterangan, ' #', get_reference) INTO get_keterangan;
	
	CALL add_audit_trail(in_type_doc, in_trans_no, in_created_by, in_id_menu, get_nama_menu, in_keterangan, get_keterangan, in_kode_perusahaan);
	
	COMMIT;
	
	SELECT 200 as `status`;

END