--
-- PostgreSQL database dump
--

\restrict u9XsRFZS6tF51bfKWkDKMXtMUcfO4E9GhibWRNKCDaklXEf0xKbaY1B56dt5LO8

-- Dumped from database version 18.6
-- Dumped by pg_dump version 18.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'SQL_ASCII';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: trg_check_label_scope_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trg_check_label_scope_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF OLD.scope = 'SERVER' AND NEW.scope <> 'SERVER' THEN
        IF EXISTS (SELECT 1 FROM user_label_policies WHERE label_id = OLD.id) THEN
            RAISE EXCEPTION 'Cannot change scope of label referenced in user_label_policies';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;


--
-- Name: trg_check_user_label_policy_scope(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trg_check_user_label_policy_scope() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_scope TEXT;
BEGIN
    SELECT scope INTO v_scope FROM labels WHERE id = NEW.label_id FOR SHARE;
    IF v_scope IS DISTINCT FROM 'SERVER' THEN
        RAISE EXCEPTION 'user_label_policies only permit SERVER labels, got % for label_id %', v_scope, NEW.label_id;
    END IF;
    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: credentials; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.credentials (
    id text NOT NULL,
    user_id text NOT NULL,
    kind text NOT NULL,
    scopes text[] DEFAULT '{}'::text[] NOT NULL,
    secret_hash text,
    hash_scheme text,
    external_subject text,
    label text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_used_at timestamp with time zone,
    expires_at timestamp with time zone,
    revoked_at timestamp with time zone,
    CONSTRAINT credentials_check CHECK (((kind <> 'PAT'::text) OR (cardinality(scopes) > 0)))
);


--
-- Name: domain_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.domain_events (
    id bigint NOT NULL,
    published_sequence bigint,
    published_at timestamp with time zone,
    kind text NOT NULL,
    version integer DEFAULT 1 NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT domain_events_check CHECK (((published_sequence IS NULL) = (published_at IS NULL)))
);


--
-- Name: domain_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.domain_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: domain_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.domain_events_id_seq OWNED BY public.domain_events.id;


--
-- Name: event_subscribers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.event_subscribers (
    id text NOT NULL,
    kind text NOT NULL,
    "position" bigint DEFAULT 0 NOT NULL,
    state text DEFAULT 'ACTIVE'::text NOT NULL,
    stalled_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT event_subscribers_check CHECK (((state = 'STALLED'::text) = (stalled_at IS NOT NULL))),
    CONSTRAINT event_subscribers_state_check CHECK ((state = ANY (ARRAY['ACTIVE'::text, 'PAUSED'::text, 'STALLED'::text])))
);


--
-- Name: item_labels; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.item_labels (
    label_id text NOT NULL,
    item_type text NOT NULL,
    item_id text NOT NULL,
    CONSTRAINT item_labels_item_type_check CHECK ((item_type = ANY (ARRAY['SERIES'::text, 'VOLUME'::text, 'ASSET'::text])))
);


--
-- Name: jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.jobs (
    id text NOT NULL,
    kind text NOT NULL,
    state text DEFAULT 'QUEUED'::text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    checkpoint jsonb DEFAULT '{}'::jsonb NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    max_attempts integer DEFAULT 3 NOT NULL,
    cancel_requested boolean DEFAULT false NOT NULL,
    last_error text,
    created_by text,
    dedupe_key text,
    run_after timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT jobs_state_check CHECK ((state = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'SUCCEEDED'::text, 'FAILED'::text, 'CANCELLED'::text])))
);


--
-- Name: labels; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.labels (
    id text NOT NULL,
    scope text NOT NULL,
    owner_id text,
    name text NOT NULL,
    built_in boolean DEFAULT false NOT NULL,
    CONSTRAINT labels_check CHECK (((scope = 'SERVER'::text) = (owner_id IS NULL))),
    CONSTRAINT labels_scope_check CHECK ((scope = ANY (ARRAY['SERVER'::text, 'USER'::text])))
);


--
-- Name: role_permissions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.role_permissions (
    role_id text NOT NULL,
    permission text NOT NULL
);


--
-- Name: roles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.roles (
    id text NOT NULL,
    name text NOT NULL,
    built_in boolean DEFAULT false NOT NULL,
    requires_allow_labels boolean DEFAULT false NOT NULL
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version bigint NOT NULL,
    name text NOT NULL,
    checksum text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: system_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.system_settings (
    key text NOT NULL,
    value jsonb NOT NULL,
    is_secret boolean DEFAULT false NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by text
);


--
-- Name: user_label_policies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_label_policies (
    user_id text NOT NULL,
    label_id text NOT NULL,
    effect text NOT NULL,
    CONSTRAINT user_label_policies_effect_check CHECK ((effect = ANY (ARRAY['ALLOW'::text, 'RESTRICT'::text])))
);


--
-- Name: user_roles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_roles (
    user_id text NOT NULL,
    role_id text NOT NULL
);


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id text NOT NULL,
    username text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: domain_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.domain_events ALTER COLUMN id SET DEFAULT nextval('public.domain_events_id_seq'::regclass);


--
-- Data for Name: credentials; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.credentials VALUES ('019318a0-0000-7000-8000-000000000020', '019318a0-0000-7000-8000-000000000010', 'PASSWORD', '{}', '$argon2id$v=19$m=65536,t=3,p=4$adminhash', 'argon2id', NULL, 'Admin password', '2026-11-10 21:00:00+09', NULL, NULL, NULL);
INSERT INTO public.credentials VALUES ('019318a0-0000-7000-8000-000000000021', '019318a0-0000-7000-8000-000000000010', 'PAT', '{read,progress:write,metadata:write,library:admin,admin}', '$argon2id$v=19$m=65536,t=3,p=4$patadminhash', 'argon2id', NULL, 'Admin PAT', '2026-11-10 21:05:00+09', NULL, NULL, NULL);
INSERT INTO public.credentials VALUES ('019318a0-0000-7000-8000-000000000022', '019318a0-0000-7000-8000-000000000011', 'PAT', '{read,progress:write}', '$argon2id$v=19$m=65536,t=3,p=4$patalicehash', 'argon2id', NULL, 'Mihon on Pixel', '2026-11-10 21:10:00+09', NULL, NULL, NULL);
INSERT INTO public.credentials VALUES ('019318a0-0000-7000-8000-000000000023', '019318a0-0000-7000-8000-000000000012', 'OIDC_LINK', '{}', NULL, NULL, 'https://auth.example.com|bob-subject-id', 'Authelia SSO', '2026-11-10 21:15:00+09', NULL, NULL, NULL);


--
-- Data for Name: domain_events; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.domain_events VALUES (1, 1, '2026-11-10 21:00:00+09', 'user.created', 1, 'user', '019318a0-0000-7000-8000-000000000010', '{"username": "admin"}', '2026-11-10 21:00:00+09');
INSERT INTO public.domain_events VALUES (2, NULL, NULL, 'job.started', 1, 'job', '019318a0-0000-7000-8000-000000000040', '{"kind": "SCAN"}', '2026-11-10 21:30:00+09');


--
-- Data for Name: event_subscribers; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.event_subscribers VALUES ('notify:shoutrrr', 'shoutrrr', 1, 'ACTIVE', NULL, '2026-11-10 21:00:00+09');
INSERT INTO public.event_subscribers VALUES ('webhook:discord', 'webhook', 0, 'PAUSED', NULL, '2026-11-10 21:05:00+09');


--
-- Data for Name: item_labels; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.item_labels VALUES ('019318a0-0000-7000-8000-000000000031', 'SERIES', '019318a0-0000-7000-8000-000000000090');
INSERT INTO public.item_labels VALUES ('019318a0-0000-7000-8000-000000000032', 'SERIES', '019318a0-0000-7000-8000-000000000091');
INSERT INTO public.item_labels VALUES ('019318a0-0000-7000-8000-000000000033', 'VOLUME', '019318a0-0000-7000-8000-000000000092');


--
-- Data for Name: jobs; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.jobs VALUES ('019318a0-0000-7000-8000-000000000040', 'SCAN', 'QUEUED', '{"library_id": "019318a0-0000-7000-8000-000000000080"}', '{}', 0, 3, false, NULL, '019318a0-0000-7000-8000-000000000010', 'SCAN:019318a0-0000-7000-8000-000000000080', '2026-11-10 21:30:00+09', '2026-11-10 21:30:00+09', NULL, '2026-11-10 21:30:00+09', NULL);
INSERT INTO public.jobs VALUES ('019318a0-0000-7000-8000-000000000041', 'PURGE', 'SUCCEEDED', '{"items_purged": 3}', '{"stage": "completed"}', 1, 3, false, NULL, '019318a0-0000-7000-8000-000000000010', NULL, '2026-11-10 21:35:00+09', '2026-11-10 21:35:00+09', '2026-11-10 21:35:01+09', '2026-11-10 21:35:05+09', '2026-11-10 21:35:05+09');


--
-- Data for Name: labels; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.labels VALUES ('019318a0-0000-7000-8000-000000000030', 'SERVER', NULL, 'marked_for_deletion', true);
INSERT INTO public.labels VALUES ('019318a0-0000-7000-8000-000000000031', 'SERVER', NULL, 'kids-safe', false);
INSERT INTO public.labels VALUES ('019318a0-0000-7000-8000-000000000032', 'SERVER', NULL, 'mature-content', false);
INSERT INTO public.labels VALUES ('019318a0-0000-7000-8000-000000000033', 'USER', '019318a0-0000-7000-8000-000000000011', 'favorite', true);
INSERT INTO public.labels VALUES ('019318a0-0000-7000-8000-000000000034', 'USER', '019318a0-0000-7000-8000-000000000011', 'want-to-read', true);


--
-- Data for Name: role_permissions; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000001', 'library:read');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000001', 'progress:write');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000001', 'metadata:edit');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000001', 'collections:manage');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000001', 'library:admin');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000001', 'users:admin');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000001', 'jobs:read_all');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000001', 'trash:purge');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000002', 'library:read');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000002', 'progress:write');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000002', 'metadata:edit');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000002', 'collections:manage');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000003', 'library:read');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000003', 'progress:write');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000004', 'library:read');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000004', 'progress:write');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000005', 'library:read');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000005', 'metadata:edit');


--
-- Data for Name: roles; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.roles VALUES ('019318a0-0000-7000-8000-000000000001', 'Admin', true, false);
INSERT INTO public.roles VALUES ('019318a0-0000-7000-8000-000000000002', 'Curator', true, false);
INSERT INTO public.roles VALUES ('019318a0-0000-7000-8000-000000000003', 'Reader', true, false);
INSERT INTO public.roles VALUES ('019318a0-0000-7000-8000-000000000004', 'Restricted reader', true, true);
INSERT INTO public.roles VALUES ('019318a0-0000-7000-8000-000000000005', 'Junior Curator', false, false);


--
-- Data for Name: schema_migrations; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.schema_migrations VALUES (1, '0001_init.sql', 'sha256:cf57a7fc15c3e5604c631a18a05139b22052d0ca876260ac1cf69cdeb6cea440', '2026-11-11 16:47:14.057247+09');


--
-- Data for Name: system_settings; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.system_settings VALUES ('oidc.default_role_id', '"019318a0-0000-7000-8000-000000000003"', false, '2026-11-11 16:47:14.073047+09', NULL);
INSERT INTO public.system_settings VALUES ('backup.passphrase', '"Y2hhbmdlbWUtc2VjcmV0"', true, '2026-11-11 16:47:14.073047+09', NULL);
INSERT INTO public.system_settings VALUES ('events.retention_days', '7', false, '2026-11-11 16:47:14.073047+09', '019318a0-0000-7000-8000-000000000010');


--
-- Data for Name: user_label_policies; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.user_label_policies VALUES ('019318a0-0000-7000-8000-000000000012', '019318a0-0000-7000-8000-000000000031', 'ALLOW');
INSERT INTO public.user_label_policies VALUES ('019318a0-0000-7000-8000-000000000011', '019318a0-0000-7000-8000-000000000032', 'RESTRICT');


--
-- Data for Name: user_roles; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.user_roles VALUES ('019318a0-0000-7000-8000-000000000010', '019318a0-0000-7000-8000-000000000001');
INSERT INTO public.user_roles VALUES ('019318a0-0000-7000-8000-000000000011', '019318a0-0000-7000-8000-000000000003');
INSERT INTO public.user_roles VALUES ('019318a0-0000-7000-8000-000000000012', '019318a0-0000-7000-8000-000000000004');


--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.users VALUES ('019318a0-0000-7000-8000-000000000010', 'admin', '2026-11-10 21:00:00+09', '2026-11-10 21:00:00+09');
INSERT INTO public.users VALUES ('019318a0-0000-7000-8000-000000000011', 'alice', '2026-11-10 21:01:00+09', '2026-11-10 21:01:00+09');
INSERT INTO public.users VALUES ('019318a0-0000-7000-8000-000000000012', 'bob', '2026-11-10 21:02:00+09', '2026-11-10 21:02:00+09');


--
-- Name: domain_events_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.domain_events_id_seq', 2, true);


--
-- Name: credentials credentials_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.credentials
    ADD CONSTRAINT credentials_pkey PRIMARY KEY (id);


--
-- Name: domain_events domain_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.domain_events
    ADD CONSTRAINT domain_events_pkey PRIMARY KEY (id);


--
-- Name: domain_events domain_events_published_sequence_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.domain_events
    ADD CONSTRAINT domain_events_published_sequence_key UNIQUE (published_sequence);


--
-- Name: event_subscribers event_subscribers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.event_subscribers
    ADD CONSTRAINT event_subscribers_pkey PRIMARY KEY (id);


--
-- Name: item_labels item_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.item_labels
    ADD CONSTRAINT item_labels_pkey PRIMARY KEY (label_id, item_type, item_id);


--
-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);


--
-- Name: labels labels_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.labels
    ADD CONSTRAINT labels_pkey PRIMARY KEY (id);


--
-- Name: role_permissions role_permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.role_permissions
    ADD CONSTRAINT role_permissions_pkey PRIMARY KEY (role_id, permission);


--
-- Name: roles roles_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_name_key UNIQUE (name);


--
-- Name: roles roles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: system_settings system_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.system_settings
    ADD CONSTRAINT system_settings_pkey PRIMARY KEY (key);


--
-- Name: user_label_policies user_label_policies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_label_policies
    ADD CONSTRAINT user_label_policies_pkey PRIMARY KEY (user_id, label_id);


--
-- Name: user_roles user_roles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_roles
    ADD CONSTRAINT user_roles_pkey PRIMARY KEY (user_id, role_id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users users_username_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_username_key UNIQUE (username);


--
-- Name: idx_credentials_oidc; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_credentials_oidc ON public.credentials USING btree (external_subject) WHERE (kind = 'OIDC_LINK'::text);


--
-- Name: idx_credentials_password; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_credentials_password ON public.credentials USING btree (user_id) WHERE ((kind = 'PASSWORD'::text) AND (revoked_at IS NULL));


--
-- Name: idx_domain_events_unpublished; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_domain_events_unpublished ON public.domain_events USING btree (id) WHERE (published_sequence IS NULL);


--
-- Name: idx_item_labels_item; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_item_labels_item ON public.item_labels USING btree (item_type, item_id);


--
-- Name: idx_jobs_active_dedupe; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_jobs_active_dedupe ON public.jobs USING btree (dedupe_key) WHERE ((dedupe_key IS NOT NULL) AND (state = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text])));


--
-- Name: idx_jobs_runnable; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_jobs_runnable ON public.jobs USING btree (kind, run_after) WHERE (state = 'QUEUED'::text);


--
-- Name: idx_labels_server; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_labels_server ON public.labels USING btree (name) WHERE (scope = 'SERVER'::text);


--
-- Name: idx_labels_user; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_labels_user ON public.labels USING btree (owner_id, name) WHERE (scope = 'USER'::text);


--
-- Name: labels trg_labels_prevent_server_scope_change; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER trg_labels_prevent_server_scope_change AFTER UPDATE OF scope ON public.labels DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION public.trg_check_label_scope_update();


--
-- Name: user_label_policies trg_user_label_policies_server_only; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER trg_user_label_policies_server_only AFTER INSERT OR UPDATE ON public.user_label_policies DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION public.trg_check_user_label_policy_scope();


--
-- Name: credentials credentials_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.credentials
    ADD CONSTRAINT credentials_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: system_settings fk_system_settings_updated_by; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.system_settings
    ADD CONSTRAINT fk_system_settings_updated_by FOREIGN KEY (updated_by) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: item_labels item_labels_label_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.item_labels
    ADD CONSTRAINT item_labels_label_id_fkey FOREIGN KEY (label_id) REFERENCES public.labels(id) ON DELETE CASCADE;


--
-- Name: jobs jobs_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: labels labels_owner_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.labels
    ADD CONSTRAINT labels_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: role_permissions role_permissions_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.role_permissions
    ADD CONSTRAINT role_permissions_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.roles(id) ON DELETE CASCADE;


--
-- Name: user_label_policies user_label_policies_label_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_label_policies
    ADD CONSTRAINT user_label_policies_label_id_fkey FOREIGN KEY (label_id) REFERENCES public.labels(id) ON DELETE RESTRICT;


--
-- Name: user_label_policies user_label_policies_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_label_policies
    ADD CONSTRAINT user_label_policies_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: user_roles user_roles_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_roles
    ADD CONSTRAINT user_roles_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.roles(id) ON DELETE CASCADE;


--
-- Name: user_roles user_roles_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_roles
    ADD CONSTRAINT user_roles_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict u9XsRFZS6tF51bfKWkDKMXtMUcfO4E9GhibWRNKCDaklXEf0xKbaY1B56dt5LO8

