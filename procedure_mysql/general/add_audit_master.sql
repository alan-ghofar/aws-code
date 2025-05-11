CREATE DEFINER=`permen`@`%` PROCEDURE `add_audit_master`( in_id_menu INT, in_nama_menu VARCHAR(50), in_action VARCHAR(50), in_keterangan TEXT, in_user_id INT, in_id_master INT, in_nama_master VARCHAR(50), in_nama_tabel VARCHAR(50), in_kode_perusahaan VARCHAR(50))
BEGIN
	set time_zone = 'Asia/Jakarta';
	INSERT INTO sc_01_audit_master (id_menu, nama_menu, action, keterangan, tanggal, user_id, id_master, nama_master, nama_tabel, kode_perusahaan) 
	VALUES(in_id_menu, in_nama_menu, in_action, in_keterangan, NOW(), in_user_id, in_id_master, in_nama_master, in_nama_tabel, in_kode_perusahaan);

END