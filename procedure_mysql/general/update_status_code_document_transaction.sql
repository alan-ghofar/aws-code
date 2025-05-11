CREATE DEFINER=`permen`@`%` PROCEDURE `update_status_code_document_transaction`(in_type_doc VARCHAR(100), in_trans_no INT, in_status_code_document VARCHAR(50), in_line_items TEXT, in_create_by INT)
BEGIN
	DECLARE get_ov_amount DOUBLE;
	set time_zone = 'Asia/Jakarta';
	CASE in_type_doc 
	WHEN '1001'	THEN
	
		IF in_line_items <> '' THEN
			-- update quantity di table detail transaksi berdasarkan table temporary dari transaksi approval
			UPDATE temp_line_items temp 
			LEFT JOIN sl_02_t_sales_quotation_details sqd 
				ON (temp.id_line_detail_temp = sqd.id
				AND temp.stock_code_temp = sqd.item_code)
			SET sqd.quantity = temp.quantity_temp
			WHERE sqd.type_doc = in_type_doc
				AND sqd.trans_no  = in_trans_no;
			
			-- get total yg baru
			SELECT SUM(sqd.unit_price * sqd.quantity) as total_amount INTO get_ov_amount 
			FROM sl_02_t_sales_quotation_details sqd
			WHERE sqd.type_doc = in_type_doc AND sqd.trans_no = in_trans_no;
		
			UPDATE sl_01_t_sales_quotation 
			SET status_code = in_status_code_document,
			ov_amount = get_ov_amount,
			update_by = in_create_by,
			update_date = CURDATE()
			WHERE type_doc = in_type_doc AND trans_no = in_trans_no;
		
			UPDATE sl_01_t_customer_trans 
			SET status_code = in_status_code_document,
			ov_amount = get_ov_amount,
			update_by = in_create_by,
			update_date = CURDATE()
			WHERE type_doc = in_type_doc AND trans_no = in_trans_no;
		ELSE
			UPDATE sl_01_t_sales_quotation 
			SET status_code = in_status_code_document,
			update_by = in_create_by,
			update_date = CURDATE()
			WHERE type_doc = in_type_doc AND trans_no = in_trans_no;
		
			UPDATE sl_01_t_customer_trans 
			SET status_code = in_status_code_document,
			update_by = in_create_by,
			update_date = CURDATE()
			WHERE type_doc = in_type_doc AND trans_no = in_trans_no;
		END IF;
		
	WHEN '1002' THEN
		UPDATE sl_01_t_sales_order  
		SET status_code = in_status_code_document,
		update_by = in_create_by,
		update_date = CURDATE()
		WHERE type_doc = in_type_doc AND trans_no = in_trans_no;
	
		update sl_01_t_customer_trans set status_code = in_status_code_document
		where type_doc = in_type_doc and trans_no = in_trans_no;
	
		IF in_line_items <> '' THEN
			-- update quantity di table detail transaksi
			UPDATE st_04_document_approval_log sdal
			LEFT JOIN sl_02_t_sales_order_details stsod 
			ON (sdal.trans_no = stsod.trans_no
				AND sdal.type_doc = stsod.type_doc
				AND sdal.id_det_trans = stsod.id)
			SET stsod.quantity = sdal.quantity
			WHERE sdal.type_doc = in_type_doc
				AND sdal.trans_no = in_trans_no;
			
			-- update quanity so detail stock
			
		END IF;
		
	WHEN '1201' THEN
		update in_01_t_adjustment  set status_code = in_status_code_document
		where type_doc = in_type_doc and trans_no = in_trans_no;
		
	WHEN '1202' THEN
		update in_01_t_adjustment  set status_code = in_status_code_document
		where type_doc = in_type_doc and trans_no = in_trans_no;
	
	WHEN '1204' THEN
		update in_01_t_location_transfer  set status_code = in_status_code_document
		where type_doc = in_type_doc and trans_no = in_trans_no;

	WHEN '1205' THEN
		update in_01_t_location_transfer  set status_code = in_status_code_document
		where type_doc = in_type_doc and trans_no = in_trans_no;
		
	END CASE;
END