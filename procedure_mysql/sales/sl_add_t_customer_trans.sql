CREATE DEFINER=`permen`@`%` PROCEDURE `sl_add_t_customer_trans`(in_trans_no INT, in_type_doc VARCHAR(100), in_reference VARCHAR(100), in_customer_code INT, in_customer_ref VARCHAR(255), in_branch_code INT, in_trans_date DATE, in_ov_amount DOUBLE, in_ov_gst DOUBLE, in_ov_freight DOUBLE, in_ov_gst_freight DOUBLE, in_ov_discount DOUBLE, in_kode_lokasi VARCHAR(50), in_salesman_id INT, in_status_code VARCHAR(50), in_kode_perusahaan VARCHAR(50), in_create_by INT)
BEGIN
	DECLARE db_provinsi_code INT;
	DECLARE db_kabupaten_code INT;
	DECLARE db_kecamatan_code INT;
	
	SELECT provinsi_code, kabupaten_code, kecamatan_code INTO db_provinsi_code, db_kabupaten_code, db_kecamatan_code FROM sl_03_m_customer_branch WHERE branch_code = in_branch_code AND customer_code = in_customer_code;

	INSERT INTO sl_01_t_customer_trans
(trans_no, type_doc, reference, customer_code, customer_ref, branch_code, trans_date, ov_amount, ov_gst, ov_freight, ov_gst_freight, ov_discount, provinsi_code, kabupaten_code, kecamatan_code, kode_lokasi, salesman_id, status_code, kode_perusahaan, create_by, create_date)
VALUES(in_trans_no, in_type_doc, in_reference, in_customer_code, in_customer_ref, in_branch_code, in_trans_date, in_ov_amount, in_ov_gst, in_ov_freight, in_ov_gst_freight, in_ov_discount, db_provinsi_code, db_kabupaten_code, db_kecamatan_code, in_kode_lokasi, in_salesman_id, in_status_code, in_kode_perusahaan, in_create_by, NOW());

END