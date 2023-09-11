-- Copyright (c) The Parus RX Authors. All rights reserved.
-- Licensed under the MIT License.

-- ************************************************************************************************
-- CREATE TABLE prxmbdata
-- NOTE: 
--   This table is used to store the data for a message that is being processed by the proxy.
-- ************************************************************************************************
CREATE TABLE IF NOT EXISTS prxmbdata (
  ident         numeric(17) NOT NULL,
  authid        varchar(30) NOT NULL,
  connect_ext   varchar(255) NOT NULL,
  id            varchar(36) NOT NULL,
  request       bytea NOT NULL,
  response      bytea,
  status        numeric(1) DEFAULT 0 NOT NULL
                CONSTRAINT c_prxmbdata_status_val CHECK (status IN (0, 1, 2)),
  note          varchar(4000)
                CONSTRAINT c_prxmbdata_note_nb CHECK (note IS NULL OR rtrim(note) IS NOT NULL),
  CONSTRAINT c_prxmbdata_pk PRIMARY KEY (id)
);

COMMENT ON TABLE prxmbdata IS 'Данные шины сообщений';
COMMENT ON COLUMN prxmbdata.ident IS 'Идентификатор';
COMMENT ON COLUMN prxmbdata.authid IS 'Пользователь';
COMMENT ON COLUMN prxmbdata.connect_ext IS 'Внешний идентификатор сеанса';
COMMENT ON COLUMN prxmbdata.id IS 'Идентификатор сообщения';
COMMENT ON COLUMN prxmbdata.request IS 'Содержимое запроса';
COMMENT ON COLUMN prxmbdata.response IS 'Содержимое ответа';
COMMENT ON COLUMN prxmbdata.status IS 'Статус сообщения (0 - новый запрос, 1 - получен ответ, 2 - ошибка)';
COMMENT ON COLUMN prxmbdata.note IS 'Примечание';

-- ************************************************************************************************
-- CREATE TRIGGER t_prxmbdata_binsert
-- NOTE: 
--   This trigger is used to initialize the data for a new message.
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION t_prxmbdata_binsert() RETURNS trigger AS $$
BEGIN
  /* Инициализация */
  NEW.ident := gen_ident();
  NEW.authid := utilizer();
  NEW.connect_ext := pkg_session$get_connect_ext();

  PERFORM pkg_temp$set_temp_used('PRXMBDATA', NEW.ident);

  RETURN NEW;
END
$$ LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

COMMENT ON FUNCTION t_prxmbdata_binsert() IS 'Инициализация данных нового сообщения';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE upper(tgname) = 'T_PRXMBDATA_BINSERT') THEN
    CREATE TRIGGER t_prxmbdata_binsert BEFORE INSERT ON prxmbdata FOR EACH ROW EXECUTE PROCEDURE t_prxmbdata_binsert();
  END IF;
END $$;

-- ************************************************************************************************
-- CREATE FUNCTION p_prxmbdata_binsert
-- NOTE:
--   This function is used to base insert a new message into the message bus.
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION p_prxmbdata_binsert(
  sid             varchar,        -- идентификатор сообщения
  brequest        bytea           -- тело запроса
) RETURNS void AS $$
BEGIN
  INSERT INTO prxmbdata (id, request) VALUES (sid, brequest);
END
$$ LANGUAGE plpgsql VOLATILEs SECURITY DEFINER;

COMMENT ON FUNCTION p_prxmbdata_binsert(varchar, bytea) IS 'Базовое добавление записи шины сообщений';

-- ************************************************************************************************
-- CREATE FUNCTION p_prxmbdata_binsert_at
-- NOTE:
--   This function is used to base insert a new message into the message bus (in an autonomous transaction).
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION p_prxmbdata_binsert_at(
  sid             varchar,        -- идентификатор сообщения
  brequest        bytea           -- тело запроса
) RETURNS void AS $$
BEGIN
  PERFORM
    pkg_autonomous$execute_procedure(
      'P_PRXMBDATA_BINSERT',
      pkg_autonomous$wrap(sid, ',') ||
      pkg_autonomous$wrap(brequest),
      true
    );
END
$$ LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

COMMENT ON FUNCTION p_prxmbdata_binsert_at(varchar, bytea) IS 'Базовое добавление записи шины сообщений (в автономной транзакции)';

-- ************************************************************************************************
-- CREATE FUNCTION p_prxmbdata_bupdate
-- NOTE:
--   This function is used to base update a message in the message bus.
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION p_prxmbdata_bupdate(
  sid             varchar,        -- идентификатор сообщения
  bresponse       bytea,          -- содержимое ответа
  nstatus         numeric,        -- статус (0 - новый запрос, 1 - получен ответ, 2 - ошибка)
  snote           varchar         -- примечание
) RETURNS void AS $$
BEGIN
  UPDATE prxmbdata SET response = bresponse, status = nstatus, note = snote WHERE id = sid;

  IF NOT FOUND THEN
    PERFORM p_exception(0, 'Запись шины сообщений с идентификатором "%s" не найдена.', sid);
  END IF;
END
$$ LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

COMMENT ON FUNCTION p_prxmbdata_bupdate(varchar, bytea, numeric, varchar) IS 'Базовое исправление записи шины сообщений';

-- ************************************************************************************************
-- CREATE FUNCTION p_prxmbdata_bupdate_at
-- NOTE:
--   This function is used to base update a message in the message bus (in an autonomous transaction).
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION p_prxmbdata_bupdate_at(
  sid             varchar,        -- идентификатор сообщения
  bresponse       bytea,          -- содержимое ответа
  nstatus         numeric,        -- статус (0 - новый запрос, 1 - получен ответ, 2 - ошибка)
  snote           varchar         -- примечание
) RETURNS void AS $$
BEGIN
  PERFORM
    pkg_autonomous$execute_procedure
    (
      'P_PRXMBDATA_BUPDATE',
      pkg_autonomous$wrap(sid, ',') ||
      pkg_autonomous$wrap(bresponse, ',') ||
      pkg_autonomous$wrap(nstatus, ',') ||
      pkg_autonomous$wrap(snote),
      true
    );
END
$$ LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

COMMENT ON FUNCTION p_prxmbdata_bupdate_at(varchar, bytea, numeric, varchar) IS 'Базовое исправление записи шины сообщений (в автономной транзакции)';

-- ************************************************************************************************
-- CREATE FUNCTION pkg_prxmq_int$send
-- NOTE:
--   This function is used to send a message to the message bus.
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION pkg_prxmq_int$send(
  sbase_url       varchar,        -- базовый URL
  smethod         varchar,        -- метод запроса
  scontent_type   varchar,        -- тип контента
  scontent        varchar,        -- контент
  surl_params     varchar 
                    DEFAULT NULL  -- параметры URL
) RETURNS void AS $$
DECLARE
  rrequest        http_request;
  rresponse       http_response;
  surl            varchar(2000) := sbase_url;
  sresponse_val   varchar(2000);
BEGIN
  IF surl_params IS NOT NULL THEN
    surl := surl || '/' || surl_params;
  END IF;

  rrequest.method := smethod;
  rrequest.uri := surl;
  rrequest.content_type := scontent_type;
  rrequest.content := scontent;

  rresponse := http(rrequest);

  IF rresponse.status NOT IN (200, 201, 202) THEN
    PERFORM p_exception(0, 'Внутренняя ошибка обработки запроса: %s.', to_char(rresponse.status));
  END IF;

  sresponse_val := rresponse.content;
EXCEPTION
  WHEN OTHERS THEN
    PERFORM p_exception(0, 'Не удалось поставить сообщение в очередь: %s.', sqlerrm);
END
$$
LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION pkg_prxmq_int$send(varchar, varchar, varchar, varchar, varchar) IS 'Отправка сообщения в очередь';

-- ************************************************************************************************
-- CREATE FUNCTION pkg_prxmq$to_json
-- NOTE:
--   This function is used to convert a message to JSON.
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION pkg_prxmq$to_json(
  stopic          varchar,        -- очередь сообщений
  smessage        varchar         -- сообщение
) RETURNS varchar AS $$
BEGIN
  RETURN format('{"topic": "%s", "message": "%s"}', stopic, replace(smessage, '"', '\"'));
END
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION pkg_prxmq$to_json(varchar, varchar) IS 'Преобразование сообщения в JSON';

-- ************************************************************************************************
-- CREATE FUNCTION pkg_prxmq$send
-- NOTE:
--   This function is used to send a message to the message bus.
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION pkg_prxmq$send(
  stopic          varchar,        -- очередь сообщений
  smessage        varchar         -- сообщение
) RETURNS void AS $$
DECLARE
  sbase_url       varchar(4000);
BEGIN
  sbase_url := get_options_str('ParusRxGatewayServiceAddress');

  PERFORM pkg_prxmq_int$send(sbase_url, 'POST', 'application/json', pkg_prxmq$to_json(stopic, smessage));
END
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION pkg_prxmq$send(varchar, varchar) IS 'Отправка сообщения в очередь';

-- ************************************************************************************************
-- CREATE FUNCTION pkg_prxmb$get_request
-- NOTE:
--   This function is used to get the request body from the message bus.
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION pkg_prxmb$get_request(
  sid             varchar,        -- идентификатор сообщения
  out brequest    bytea           -- тело запроса
) AS $$
BEGIN
  SELECT request INTO brequest FROM prxmbdata WHERE id = sid;

  IF NOT FOUND THEN
    PERFORM p_exception(0, 'Запись шины сообщений с идентификатором "%s" не найдена.', sid);
  END IF;
END
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION pkg_prxmb$get_request(varchar, out bytea) IS 'Получение тела запроса из шины сообщений';

-- ************************************************************************************************
-- CREATE FUNCTION pkg_prxmb$set_request
-- NOTE:
--   This function is used to add a request to the message bus.
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION pkg_prxmb$set_request(
  brequest        bytea,          -- тело запроса
  out sid         varchar         -- идентификатор сообщения
) AS $$
BEGIN
  sid := f_sys_guid();
  PERFORM p_prxmbdata_binsert_at(sid, brequest);
END
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION pkg_prxmb$set_request(bytea, out varchar) IS 'Добавление запроса в шину сообщений';

-- ************************************************************************************************
-- CREATE FUNCTION pkg_prxmb$get_response
-- NOTE:
--   This function is used to get the response body from the message bus.
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION pkg_prxmb$get_response(
  sid             varchar,        -- идентификатор сообщения
  out bresponse   bytea           -- тело ответа
) AS $$
BEGIN
  SELECT response INTO bresponse FROM prxmbdata WHERE id = sid;

  IF NOT FOUND THEN
    PERFORM p_exception(0, 'Запись шины сообщений с идентификатором "%s" не найдена.', sid);
  END IF;
END
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION pkg_prxmb$get_response(varchar, out bytea) IS 'Получение тела ответа из шины сообщений';

-- ************************************************************************************************
-- CREATE FUNCTION pkg_prxmb$set_response
-- NOTE:
--   This function is used to add a response to the message bus.
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION pkg_prxmb$set_response(
  sid             varchar,        -- идентификатор сообщения
  bresponse       bytea           -- тело ответа
) RETURNS void AS $$
BEGIN
  PERFORM p_prxmbdata_bupdate_at(sid, bresponse, 1, NULL);
END
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION pkg_prxmb$set_response(varchar, bytea) IS 'Добавление ответа в шину сообщений';

-- ************************************************************************************************
-- CREATE FUNCTION pkg_prxmb$send
-- NOTE:
--   This function is used to send a message to the message bus and wait for a response.
-- ************************************************************************************************
CREATE OR REPLACE PROCEDURE pkg_prxmb$send(
  nflag_smart     numeric,        -- признак генерации исключения (0 - да, 1 - нет)
  stopic          varchar,        -- очередь сообщений
  sid             varchar,        -- идентификатор сообщения
  ntimeout        numeric         -- таймаут ожидания ответа (в секундах)
) RETURNS void AS $$
DECLARE
  ntime_wait      numeric;
  ntime_stamp     numeric;
  rdata           prxmbdata;
BEGIN
  PERFORM pkg_prxmq$send(stopic, sid);

  ntime_wait := greatest(coalesce(ntimeout, 0), 0);
  ntime_stamp := dbms_utility$get_time();

  LOOP
    IF ntime_wait >= 1 THEN
      PERFORM pkg_advisory_lock$sleep(1);
    END IF;

    BEGIN
      SELECT * INTO rdata FROM prxmbdata WHERE id = sid;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        PERFORM p_exception(nflag_smart, 'Запись шины сообщений с идентификатором "%s" не найдена.', sid);
    END;

    IF rdata.status != 0 THEN
      EXIT;
    END IF;

    IF ntime_wait < 1 OR (dbms_utility$get_time() - ntime_stamp) / 100 >= ntime_wait THEN
      EXIT;
    END IF;
  END LOOP;

  IF rdata.status = 0 THEN
    PERFORM p_exception(nflag_smart, 'Превышено время ожидания ответа (%s секунд) на сообщение с идентификатором "%s" в очереди сообщений "%s".', ntimeout, sid, stopic);
  END IF;

  IF rdata.status = 2 THEN
    PERFORM p_exception(nflag_smart, 'Ошибка "%s" при обработке сообщения с идентификатором "%s" в очереди сообщений "%s".', rdata.note, sid, stopic);
  END IF;
END
$$ LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

COMMENT ON PROCEDURE pkg_prxmb$send(numeric, varchar, varchar, numeric) IS 'Отправка сообщения в шину сообщений';

-- ************************************************************************************************
-- CREATE FUNCTION pkg_prxmb$set_error
-- NOTE:
--   This function is used to add an error to the message bus.
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION pkg_prxmb$set_error(
  sid             varchar,        -- идентификатор сообщения
  snote           varchar         -- ошибка
) RETURNS void AS $$
BEGIN
  PERFORM p_prxmbdata_bupdate_at(sid, NULL, 2, snote);
END
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION pkg_prxmb$set_error(varchar, varchar) IS 'Добавление ошибки в шину сообщений';

-- ************************************************************************************************
-- CREATE FUNCTION p_prx_system_init_options
-- NOTE:
--   This function is used to initialize system parameters.
-- ************************************************************************************************
CREATE OR REPLACE FUNCTION p_prx_system_init_options() RETURNS void AS $$
BEGIN
  PERFORM p_system_init_option('OptionsSystemGlobal', 'ParusRxGatewayServiceAddress', 'Parus RX Gateway Service Address', 31000000,
    1, 1, 0, 0, 0, 240, null, null, null, null, null, null, null, null, null, null, null, null);
END
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION p_prx_system_init_options() IS 'Инициализация системных параметров';

DO $$
BEGIN
  PERFORM p_prx_system_init_options();
END $$;