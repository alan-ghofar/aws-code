CREATE DEFINER=`permen`@`%` FUNCTION `generate_document_number`(in_kode_perusahaan VARCHAR(50), `in_type_doc` VARCHAR(100), `in_trans_date` DATE , `in_created_by` INT) RETURNS int(11)
BEGIN
DECLARE db_recount TINYINT;
DECLARE db_digit_number TINYINT;
DECLARE db_prefix VARCHAR(100);
DECLARE db_separator CHAR(2);
DECLARE return_trans_no INT;
DECLARE return_reference VARCHAR(200);
DECLARE next_number INT;
DECLARE bulannya INT;
DECLARE tahunnya INT;
DECLARE bulanstr VARCHAR(2);

-- ambil bulan dan tahunnya dari inputan tanggalnya
select SUBSTRING(in_trans_date, 3, 2) ini_tahun INTO tahunnya;
select SUBSTRING(in_trans_date, 6, 2) ini_bulan INTO bulannya;

/**
 ambil parameter di table document
	recount => 0 = lanjut terus / 1 = pertahun / 2 = per bulan
	digit_number => jumlah digit referencenya selain prefix dan separator
	prefix => prefix depan
	separator => pemisah antara prefix dan count reference
 **/
SELECT
	`recount`,
	`digit_number`,
	`prefix`,
	`separator`
	INTO  @db_recount, @db_digit_number, @db_prefix, @db_separator
FROM
	st_01_document  
WHERE
	type_doc = in_type_doc AND active = 1 AND delete_mark = 0 LIMIT 1;
	
-- trans no increment per tipe dokumen
SELECT (IFNULL(MAX(trans_no),0) + 1) AS trans_no  INTO return_trans_no
	FROM st_02_refs
	WHERE `type_doc` = in_type_doc;
	

CASE
	WHEN @db_recount = 0 THEN
	-- reference naik terus
	SELECT (IFNULL(MAX(refs_no),0) + 1) AS nomer  INTO next_number 
	FROM st_02_refs
	WHERE `type_doc` = in_type_doc AND `kode_perusahaan` = in_kode_perusahaan;

	-- format reference full recount 0
	SET return_reference = CONCAT(@db_prefix, @db_separator, LPAD(next_number, @db_digit_number,'0'));


	WHEN @db_recount = 1 THEN
	-- reference ulang dari 1 jika ganti tahun
	SELECT (IFNULL(MAX(refs_no),0) + 1) AS nomer  INTO next_number 
	FROM st_02_refs
	WHERE `type_doc` = in_type_doc AND `kode_perusahaan` = in_kode_perusahaan AND thn = tahunnya;

	-- format reference full recount 1 / tahunan
	SET return_reference = CONCAT(tahunnya, @db_separator, @db_prefix, @db_separator, LPAD(next_number, @db_digit_number,'0'));


	WHEN @db_recount = 2 THEN 
	-- reference ulang dari 1 jika ganti bulan 
	SELECT (IFNULL(MAX(refs_no),0) + 1) AS nomer  INTO next_number 
	FROM st_02_refs
	WHERE `type_doc` = in_type_doc AND `kode_perusahaan` = in_kode_perusahaan AND thn = tahunnya AND bln = bulannya;
	-- format bulan
	SET bulanstr = IF(bulannya<10, LPAD(bulannya,2,'0'),bulannya); 

	-- format reference full recount 2 / bulanan
	SET return_reference = CONCAT(bulanstr, @db_separator, tahunnya, @db_separator, @db_prefix, @db_separator, LPAD(next_number, @db_digit_number,'0'));
	
END CASE;

-- simpan nomer yg sudah digenerate
CALL insert_st_refs  (in_type_doc, return_trans_no, bulannya, tahunnya, @db_recount, @db_prefix, return_reference, next_number, in_kode_perusahaan, in_created_by);

	RETURN return_trans_no;
END