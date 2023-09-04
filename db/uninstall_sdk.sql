-- Copyright (c) The Parus RX Authors. All rights reserved.
-- Licensed under the MIT License.

DROP TABLE IF EXISTS prxmbdata;
DROP TRIGGER IF EXISTS t_prxmbdata_binsert ON prxmbdata;
DROP FUNCTION IF EXISTS t_prxmbdata_binsert();
DROP FUNCTION IF EXISTS p_prxmbdata_binsert(varchar, bytea);
DROP FUNCTION IF EXISTS p_prxmbdata_binsert_at(varchar, bytea);
DROP FUNCTION IF EXISTS p_prxmbdata_bupdate(varchar, bytea, numeric, varchar);
DROP FUNCTION IF EXISTS p_prxmbdata_bupdate_at(varchar, bytea, numeric, varchar);
DROP FUNCTION IF EXISTS pkg_prxmq_int$send(varchar, varchar, varchar, varchar, varchar);
DROP FUNCTION IF EXISTS pkg_prxmq$to_json(varchar, varchar);
DROP FUNCTION IF EXISTS pkg_prxmq$send(varchar, varchar);
DROP FUNCTION IF EXISTS pkg_prxmb$get_request(varchar, out bytea);
DROP FUNCTION IF EXISTS pkg_prxmb$set_request(bytea, out varchar);
DROP FUNCTION IF EXISTS pkg_prxmb$get_response(varchar, out bytea);
DROP FUNCTION IF EXISTS pkg_prxmb$set_response(varchar, bytea);
DROP FUNCTION IF EXISTS pkg_prxmb$send(numeric, varchar, varchar, numeric);
DROP FUNCTION IF EXISTS pkg_prxmb$set_error(varchar, varchar);
DROP FUNCTION IF EXISTS p_prx_system_init_options();

DO $$
BEGIN
  RAISE NOTICE 'Deleting system options';
  DELETE FROM options WHERE numb = 31000000;
END $$;