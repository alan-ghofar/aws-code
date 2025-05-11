CREATE DEFINER=`permen`@`%` PROCEDURE `add_audit_trail`( in_type_doc INT, in_trans_no INT, in_id_user INT, in_id_menu INT, in_nama_menu VARCHAR(255), in_action VARCHAR(255), in_keterangan TEXT, in_kode_perusahaan VARCHAR(50))
BEGIN
	set time_zone = 'Asia/Jakarta';
	INSERT INTO sc_01_audit_trail (type_doc, trans_no, id_user, id_menu, nama_menu, action, keterangan, tanggal, kode_perusahaan) 
	VALUES(in_type_doc, in_trans_no, in_id_user, in_id_menu, in_nama_menu, in_action, in_keterangan, NOW(), in_kode_perusahaan);

END