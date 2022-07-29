--
-- PostgreSQL database dump
--

-- Dumped from database version 14.2
-- Dumped by pg_dump version 14.2

-- Started on 2022-07-29 16:05:11

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 5 (class 2615 OID 2200)
-- Name: public; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA public;


ALTER SCHEMA public OWNER TO postgres;

--
-- TOC entry 3395 (class 0 OID 0)
-- Dependencies: 5
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA public IS 'standard public schema';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 221 (class 1259 OID 18918)
-- Name: error_log; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.error_log (
    table_name character varying(100),
    app_name character varying(100),
    error_msg text,
    error_details text,
    load_user_code character varying(32),
    load_dt timestamp without time zone DEFAULT timezone('UTC'::text, now()) NOT NULL
);


ALTER TABLE public.error_log OWNER TO postgres;

--
-- TOC entry 227 (class 1259 OID 27196)
-- Name: mdm_stuff; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.mdm_stuff (
    qlik_app_id character varying(50),
    qlik_app_name character varying(200),
    sheet_name character varying(200),
    control_type character varying(150),
    control_title text,
    control_fld_label character varying(150),
    control_fld_field text,
    qlik_tab_name text,
    qlik_app_local_qvd_name character varying(200),
    qvd_path text[],
    qlik_qvd_fld text,
    qlik_qvd_col character varying,
    qlik_qvd_fld_split_par text,
    qlik_qvd_fld_arr text,
    qlik_qvd_col_alias character varying,
    col_path jsonb,
    qlik_qvd_fld_split_chld text,
    qvd_gen_app character varying(100),
    qvd_gen_app_tab_name character varying(200),
    qlik_conn_name character varying(200),
    qlik_qvd_name character varying(100),
    src_data_entity character varying(200),
    src_col_name text
);


ALTER TABLE public.mdm_stuff OWNER TO postgres;

--
-- TOC entry 225 (class 1259 OID 21572)
-- Name: qlik_app_not_finding_lineage; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.qlik_app_not_finding_lineage (
    qlik_app_id character varying(50),
    qlik_app_name character varying(200),
    sheet_name character varying(200),
    control_type character varying(150),
    control_title text,
    control_fld_label character varying(150),
    control_fld_field text,
    qlik_tab_name text,
    qlik_app_local_qvd_name character varying(200),
    qvd_path text[],
    qlik_qvd_fld text,
    qlik_qvd_fld_split_par text,
    qlik_qvd_fld_arr text,
    qlik_qvd_col_alias character varying,
    col_path jsonb,
    qlik_qvd_fld_split_chld text,
    qvd_gen_app character varying(100),
    qvd_gen_app_tab_name text,
    qlik_qvd_name character varying(100),
    src_data_entity character varying(200),
    src_col_name text
);


ALTER TABLE public.qlik_app_not_finding_lineage OWNER TO postgres;

--
-- TOC entry 223 (class 1259 OID 19769)
-- Name: qlik_app_qvd_fld; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.qlik_app_qvd_fld (
    qlik_app_name character varying(100),
    qlik_tab_name character varying(200),
    qlik_app_local_qvd_name character varying(200),
    qlik_qvd_name character varying(100),
    qlik_qvd_col character varying,
    qlik_qvd_col_alias character varying,
    qlik_qvd_fld character varying,
    load_user_code character varying(32),
    load_dt timestamp without time zone DEFAULT timezone('UTC'::text, now()) NOT NULL
);


ALTER TABLE public.qlik_app_qvd_fld OWNER TO postgres;

--
-- TOC entry 228 (class 1259 OID 27307)
-- Name: qlik_app_qvd_fld_split; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.qlik_app_qvd_fld_split (
    qlik_app_name character varying(100),
    qlik_tab_name character varying(200),
    qlik_app_local_qvd_name character varying(200),
    qlik_qvd_name character varying(100),
    qlik_qvd_col character varying,
    qlik_qvd_col_alias character varying,
    qlik_qvd_fld character varying,
    load_user_code character varying(32),
    load_dt timestamp without time zone,
    qlik_qvd_fld_split text
);


ALTER TABLE public.qlik_app_qvd_fld_split OWNER TO postgres;

--
-- TOC entry 212 (class 1259 OID 17098)
-- Name: qlik_app_qvd_flds; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.qlik_app_qvd_flds (
    qlik_app_name character varying(100),
    qlik_tab_name character varying(200),
    qlik_app_local_qvd_name character varying(200),
    qlik_qvd_name character varying(100),
    qlik_qvd_cols character varying
);


ALTER TABLE public.qlik_app_qvd_flds OWNER TO postgres;

--
-- TOC entry 222 (class 1259 OID 19302)
-- Name: qlik_app_sheet_controls; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.qlik_app_sheet_controls (
    qlik_app_id character varying(50),
    qlik_app_name character varying(200),
    sheet_id character varying(50),
    sheet_name character varying(200),
    parent_control_id character varying(50),
    parent_control_type character varying(50),
    control_id character varying(50),
    control_type character varying(150),
    control_title text,
    control_fld_label character varying(150),
    control_fld_field text,
    load_user_code character varying(32),
    load_dt timestamp without time zone DEFAULT timezone('UTC'::text, now()) NOT NULL
);


ALTER TABLE public.qlik_app_sheet_controls OWNER TO postgres;

--
-- TOC entry 226 (class 1259 OID 26482)
-- Name: qlik_app_sheet_controls_mdm; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.qlik_app_sheet_controls_mdm (
    qlik_app_id character varying(50),
    qlik_app_name character varying(200),
    sheet_id character varying(50),
    sheet_name character varying(200),
    parent_control_id character varying(50),
    parent_control_type character varying(50),
    control_id character varying(50),
    control_type character varying(150),
    control_title text,
    control_fld_label character varying(150),
    control_fld_field text,
    load_user_code character varying(32),
    load_dt timestamp without time zone
);


ALTER TABLE public.qlik_app_sheet_controls_mdm OWNER TO postgres;

--
-- TOC entry 230 (class 1259 OID 27737)
-- Name: qvd_db_lineage; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.qvd_db_lineage (
    qlik_app_name character varying(100),
    qlik_tab_name text,
    qlik_app_local_qvd_name character varying(200),
    qvd_path text[],
    qlik_qvd_fld text,
    qlik_qvd_col character varying,
    qlik_qvd_fld_split_par text,
    qlik_qvd_fld_arr character varying,
    qlik_qvd_col_alias character varying,
    col_path jsonb,
    qlik_qvd_fld_split_chld text,
    qvd_gen_app character varying(100),
    qvd_gen_app_tab_name character varying(200),
    qlik_conn_name character varying(200),
    qlik_qvd_name character varying(100),
    src_data_entity character varying(200),
    src_col_name text
);


ALTER TABLE public.qvd_db_lineage OWNER TO postgres;

--
-- TOC entry 229 (class 1259 OID 27413)
-- Name: qvd_lineage; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.qvd_lineage (
    qlik_app_name character varying(100),
    qlik_tab_name text,
    qlik_app_local_qvd_name character varying(200),
    qlik_qvd_name text,
    qlik_qvd_col character varying,
    qlik_qvd_fld character varying,
    qlik_qvd_fld_split_par text,
    qlik_qvd_fld_split_chld text,
    qlik_qvd_col_alias character varying,
    level1 integer,
    qvd_path text[],
    col_path jsonb
);


ALTER TABLE public.qvd_lineage OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 18253)
-- Name: qvd_load_text; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.qvd_load_text (
    qlik_app_name character varying(100),
    qlik_tab_name character varying(200),
    qlik_conn_name character varying(200),
    qlik_qvd_name character varying(100),
    db_tab_name character varying(200),
    qlik_load_text text
);


ALTER TABLE public.qvd_load_text OWNER TO postgres;

--
-- TOC entry 224 (class 1259 OID 20535)
-- Name: qvd_tab_columns; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.qvd_tab_columns (
    qlik_app_name character varying(100),
    qlik_tab_name character varying(200),
    qlik_conn_name character varying(200),
    qlik_qvd_name character varying(100),
    db_tab_name character varying(200),
    db_col_name text
);


ALTER TABLE public.qvd_tab_columns OWNER TO postgres;

--
-- TOC entry 216 (class 1259 OID 18210)
-- Name: qvd_tab_columns_new; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.qvd_tab_columns_new (
    qlik_app_name character varying(100),
    qlik_tab_name character varying(200),
    qlik_conn_name character varying(200),
    qlik_qvd_name character varying(100),
    db_tab_name character varying(200),
    db_col_name character varying(200)
);


ALTER TABLE public.qvd_tab_columns_new OWNER TO postgres;

--
-- TOC entry 3249 (class 1259 OID 21533)
-- Name: qlik_app_sheet_controls_qlik_app_name_control_fld_field; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX qlik_app_sheet_controls_qlik_app_name_control_fld_field ON public.qlik_app_sheet_controls USING btree (qlik_app_name varchar_pattern_ops, control_fld_field varchar_pattern_ops);


--
-- TOC entry 3250 (class 1259 OID 21530)
-- Name: qvd_tab_columns_db_col_name_like; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX qvd_tab_columns_db_col_name_like ON public.qvd_tab_columns USING btree (db_col_name varchar_pattern_ops);


-- Completed on 2022-07-29 16:05:18

--
-- PostgreSQL database dump complete
--

