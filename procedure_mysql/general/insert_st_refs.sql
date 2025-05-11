CREATE DEFINER=`permen`@`%` PROCEDURE `insert_st_refs`( in_type_doc VARCHAR(100), in_trans_no INT, in_bln INT, in_thn YEAR, in_recount INT, in_prefix VARCHAR(100), in_reference VARCHAR(100), in_refs_no INT, in_kode_perusahaan VARCHAR(50), in_created_by INT)
BEGIN

-- simpan nomer yg sudah digenerate
INSERT INTO st_02_refs (type_doc, trans_no, bln, thn, recount, prefix, refs, refs_no, kode_perusahaan, create_by, create_date)
VALUES (in_type_doc, in_trans_no, in_bln, in_thn, in_recount, in_prefix, in_reference, in_refs_no, in_kode_perusahaan, in_created_by, NOW());

END