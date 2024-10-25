--
-- PostgreSQL database dump
--

-- Dumped from database version 16.2 (Postgres.app)
-- Dumped by pg_dump version 16.2 (Postgres.app)

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

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: tbl_cliente_contatos; Type: TABLE; Schema: public; Owner: postgres
--

DROP TABLE IF EXISTS public.tbl_cliente_contatos;

CREATE TABLE public.tbl_cliente_contatos (
    id bigint NOT NULL,
    cliente_id bigint NOT NULL,
    tipo_contato_id integer NOT NULL,
    contato character varying(255) NOT NULL
);


ALTER TABLE public.tbl_cliente_contatos OWNER TO postgres;

--
-- Name: tbl_cliente_contatos_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

DROP SEQUENCE IF EXISTS public.tbl_cliente_contatos_id_seq;

CREATE SEQUENCE public.tbl_cliente_contatos_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.tbl_cliente_contatos_id_seq OWNER TO postgres;

--
-- Name: tbl_cliente_contatos_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.tbl_cliente_contatos_id_seq OWNED BY public.tbl_cliente_contatos.id;


--
-- Name: tbl_cliente_contratos; Type: TABLE; Schema: public; Owner: postgres
--

DROP TABLE IF EXISTS public.tbl_cliente_contratos;

CREATE TABLE public.tbl_cliente_contratos (
    id bigint NOT NULL,
    cliente_id bigint NOT NULL,
    plano_id integer NOT NULL,
    dia_vencimento integer NOT NULL,
    isento boolean DEFAULT false NOT NULL,
    endereco_logradouro character varying(255) NOT NULL,
    endereco_numero character varying(15),
    endereco_bairro character varying(255) NOT NULL,
    endereco_cidade character varying(255) NOT NULL,
    endereco_complemento character varying(500),
    endereco_cep character varying(9) NOT NULL,
    endereco_uf character varying(2) NOT NULL,
    status_id integer NOT NULL
);


ALTER TABLE public.tbl_cliente_contratos OWNER TO postgres;

--
-- Name: tbl_cliente_contratos_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

DROP SEQUENCE IF EXISTS public.tbl_cliente_contratos_id_seq;

CREATE SEQUENCE public.tbl_cliente_contratos_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.tbl_cliente_contratos_id_seq OWNER TO postgres;

--
-- Name: tbl_cliente_contratos_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.tbl_cliente_contratos_id_seq OWNED BY public.tbl_cliente_contratos.id;


--
-- Name: tbl_clientes; Type: TABLE; Schema: public; Owner: postgres
--

DROP TABLE IF EXISTS public.tbl_clientes;

CREATE TABLE public.tbl_clientes (
    id bigint NOT NULL,
    nome_razao_social character varying(255) NOT NULL,
    nome_fantasia character varying(255),
    cpf_cnpj character varying(18) NOT NULL,
    data_nascimento date,
    data_cadastro timestamp without time zone
);


ALTER TABLE public.tbl_clientes OWNER TO postgres;

--
-- Name: tbl_clientes_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

DROP SEQUENCE IF EXISTS public.tbl_clientes_id_seq;

CREATE SEQUENCE public.tbl_clientes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.tbl_clientes_id_seq OWNER TO postgres;

--
-- Name: tbl_clientes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.tbl_clientes_id_seq OWNED BY public.tbl_clientes.id;


--
-- Name: tbl_planos; Type: TABLE; Schema: public; Owner: postgres
--

DROP TABLE IF EXISTS public.tbl_planos;

CREATE TABLE public.tbl_planos (
    id integer NOT NULL,
    descricao character varying(255) NOT NULL,
    valor numeric(15,2) NOT NULL
);


ALTER TABLE public.tbl_planos OWNER TO postgres;

--
-- Name: tbl_planos_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

DROP SEQUENCE IF EXISTS public.tbl_planos_id_seq;

CREATE SEQUENCE public.tbl_planos_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.tbl_planos_id_seq OWNER TO postgres;

--
-- Name: tbl_planos_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.tbl_planos_id_seq OWNED BY public.tbl_planos.id;


--
-- Name: tbl_status_contrato; Type: TABLE; Schema: public; Owner: postgres
--

DROP TABLE IF EXISTS public.tbl_status_contrato;

CREATE TABLE public.tbl_status_contrato (
    id integer NOT NULL,
    status character varying(50) NOT NULL
);


ALTER TABLE public.tbl_status_contrato OWNER TO postgres;

--
-- Name: tbl_status_contrato_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

DROP SEQUENCE IF EXISTS public.tbl_status_contrato_id_seq;

CREATE SEQUENCE public.tbl_status_contrato_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.tbl_status_contrato_id_seq OWNER TO postgres;

--
-- Name: tbl_status_contrato_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.tbl_status_contrato_id_seq OWNED BY public.tbl_status_contrato.id;


--
-- Name: tbl_tipos_contato; Type: TABLE; Schema: public; Owner: postgres
--

DROP TABLE IF EXISTS public.tbl_tipos_contato;

CREATE TABLE public.tbl_tipos_contato (
    id integer NOT NULL,
    tipo_contato character varying(50) NOT NULL
);


ALTER TABLE public.tbl_tipos_contato OWNER TO postgres;

--
-- Name: tbl_tipos_contato_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

DROP SEQUENCE IF EXISTS public.tbl_tipos_contato_id_seq;

CREATE SEQUENCE public.tbl_tipos_contato_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.tbl_tipos_contato_id_seq OWNER TO postgres;

--
-- Name: tbl_tipos_contato_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.tbl_tipos_contato_id_seq OWNED BY public.tbl_tipos_contato.id;


--
-- Name: tbl_cliente_contatos id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_cliente_contatos ALTER COLUMN id SET DEFAULT nextval('public.tbl_cliente_contatos_id_seq'::regclass);


--
-- Name: tbl_cliente_contratos id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_cliente_contratos ALTER COLUMN id SET DEFAULT nextval('public.tbl_cliente_contratos_id_seq'::regclass);


--
-- Name: tbl_clientes id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_clientes ALTER COLUMN id SET DEFAULT nextval('public.tbl_clientes_id_seq'::regclass);


--
-- Name: tbl_planos id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_planos ALTER COLUMN id SET DEFAULT nextval('public.tbl_planos_id_seq'::regclass);


--
-- Name: tbl_status_contrato id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_status_contrato ALTER COLUMN id SET DEFAULT nextval('public.tbl_status_contrato_id_seq'::regclass);


--
-- Name: tbl_tipos_contato id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_tipos_contato ALTER COLUMN id SET DEFAULT nextval('public.tbl_tipos_contato_id_seq'::regclass);


--
-- Data for Name: tbl_cliente_contatos; Type: TABLE DATA; Schema: public; Owner: postgres
--

-- COPY public.tbl_cliente_contatos (id, cliente_id, tipo_contato_id, contato) FROM stdin;
-- \.


--
-- Data for Name: tbl_cliente_contratos; Type: TABLE DATA; Schema: public; Owner: postgres
--

-- COPY public.tbl_cliente_contratos (id, cliente_id, plano_id, dia_vencimento, isento, endereco_logradouro, endereco_numero, endereco_bairro, endereco_cidade, endereco_complemento, endereco_cep, endereco_uf, status_id) FROM stdin;
-- \.


--
-- Data for Name: tbl_clientes; Type: TABLE DATA; Schema: public; Owner: postgres
--

-- COPY public.tbl_clientes (id, nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro) FROM stdin;
-- \.


--
-- Data for Name: tbl_planos; Type: TABLE DATA; Schema: public; Owner: postgres
--

-- COPY public.tbl_planos (id, descricao, valor) FROM stdin;
-- \.


--
-- Data for Name: tbl_status_contrato; Type: TABLE DATA; Schema: public; Owner: postgres
--

-- COPY public.tbl_status_contrato (id, status) FROM stdin;
-- 1	Ativo
-- 2	Velocidade Reduzida
-- 3	Suspenso
-- 4	Cancelado
-- \.

INSERT INTO public.tbl_status_contrato (id, status) VALUES
(1, 'Ativo'),
(2, 'Velocidade Reduzida'),
(3, 'Suspenso'),
(4, 'Cancelado');


--
-- Data for Name: tbl_tipos_contato; Type: TABLE DATA; Schema: public; Owner: postgres
--

-- COPY public.tbl_tipos_contato (id, tipo_contato) FROM stdin;
-- 1	Telefone
-- 2	Celular
-- 3	E-Mail
-- \.

INSERT INTO public.tbl_tipos_contato (id, tipo_contato) VALUES
(1, 'Telefone'),
(2, 'Celular'),
(3, 'E-Mail');


--
-- Name: tbl_cliente_contatos_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.tbl_cliente_contatos_id_seq', 1, false);


--
-- Name: tbl_cliente_contratos_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.tbl_cliente_contratos_id_seq', 1, false);


--
-- Name: tbl_clientes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.tbl_clientes_id_seq', 1, false);


--
-- Name: tbl_planos_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.tbl_planos_id_seq', 1, false);


--
-- Name: tbl_status_contrato_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.tbl_status_contrato_id_seq', 4, true);


--
-- Name: tbl_tipos_contato_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.tbl_tipos_contato_id_seq', 3, true);


--
-- Name: tbl_cliente_contatos tbl_cliente_contatos_cliente_id_tipo_contato_id_contato_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_cliente_contatos
    ADD CONSTRAINT tbl_cliente_contatos_cliente_id_tipo_contato_id_contato_key UNIQUE (cliente_id, tipo_contato_id, contato);


--
-- Name: tbl_cliente_contatos tbl_cliente_contatos_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_cliente_contatos
    ADD CONSTRAINT tbl_cliente_contatos_pkey PRIMARY KEY (id);


--
-- Name: tbl_cliente_contratos tbl_cliente_contratos_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_cliente_contratos
    ADD CONSTRAINT tbl_cliente_contratos_pkey PRIMARY KEY (id);


--
-- Name: tbl_clientes tbl_clientes_cpf_cnpj_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_clientes
    ADD CONSTRAINT tbl_clientes_cpf_cnpj_key UNIQUE (cpf_cnpj);


--
-- Name: tbl_clientes tbl_clientes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_clientes
    ADD CONSTRAINT tbl_clientes_pkey PRIMARY KEY (id);


--
-- Name: tbl_planos tbl_planos_descricao_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_planos
    ADD CONSTRAINT tbl_planos_descricao_key UNIQUE (descricao);


--
-- Name: tbl_planos tbl_planos_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_planos
    ADD CONSTRAINT tbl_planos_pkey PRIMARY KEY (id);


--
-- Name: tbl_status_contrato tbl_status_contrato_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_status_contrato
    ADD CONSTRAINT tbl_status_contrato_pkey PRIMARY KEY (id);


--
-- Name: tbl_status_contrato tbl_status_contrato_status_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_status_contrato
    ADD CONSTRAINT tbl_status_contrato_status_key UNIQUE (status);


--
-- Name: tbl_tipos_contato tbl_tipos_contato_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_tipos_contato
    ADD CONSTRAINT tbl_tipos_contato_pkey PRIMARY KEY (id);


--
-- Name: tbl_tipos_contato tbl_tipos_contato_tipo_contato_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_tipos_contato
    ADD CONSTRAINT tbl_tipos_contato_tipo_contato_key UNIQUE (tipo_contato);


--
-- Name: tbl_cliente_contatos tbl_cliente_contatos_cliente_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_cliente_contatos
    ADD CONSTRAINT tbl_cliente_contatos_cliente_id_fkey FOREIGN KEY (cliente_id) REFERENCES public.tbl_clientes(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: tbl_cliente_contatos tbl_cliente_contatos_tipo_contato_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_cliente_contatos
    ADD CONSTRAINT tbl_cliente_contatos_tipo_contato_id_fkey FOREIGN KEY (tipo_contato_id) REFERENCES public.tbl_tipos_contato(id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: tbl_cliente_contratos tbl_cliente_contratos_cliente_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_cliente_contratos
    ADD CONSTRAINT tbl_cliente_contratos_cliente_id_fkey FOREIGN KEY (cliente_id) REFERENCES public.tbl_clientes(id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: tbl_cliente_contratos tbl_cliente_contratos_plano_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_cliente_contratos
    ADD CONSTRAINT tbl_cliente_contratos_plano_id_fkey FOREIGN KEY (plano_id) REFERENCES public.tbl_planos(id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: tbl_cliente_contratos tbl_cliente_contratos_status_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tbl_cliente_contratos
    ADD CONSTRAINT tbl_cliente_contratos_status_id_fkey FOREIGN KEY (status_id) REFERENCES public.tbl_status_contrato(id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- PostgreSQL database dump complete
--

