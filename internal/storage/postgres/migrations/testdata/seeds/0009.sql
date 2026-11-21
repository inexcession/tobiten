--
-- PostgreSQL database dump
--

\restrict JP4Kz2jKf3HSvWl8aBNfoFq6gwrkdNLOuvdeE3uJc4qN4ajhx4reX2efM8Px5II

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
-- Name: check_volume_primary_membership(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.check_volume_primary_membership(v_volume_id text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_total   BIGINT;
    v_primary BIGINT;
BEGIN
    -- Bump the parent volume's tuple version (without changing values or firing column-scoped
    -- library/preferred-asset triggers) so concurrent REPEATABLE READ membership edits serialize.
    UPDATE volumes SET updated_at = updated_at WHERE id = v_volume_id;
    IF NOT FOUND THEN
        -- Volume was deleted (e.g., ON DELETE CASCADE removed all its memberships).
        RETURN;
    END IF;

    SELECT COUNT(*), COUNT(*) FILTER (WHERE is_primary)
    INTO v_total, v_primary
    FROM series_memberships
    WHERE volume_id = v_volume_id;

    IF v_total > 0 AND v_primary <> 1 THEN
        RAISE EXCEPTION 'volume % with series memberships must have exactly one primary membership (found % primary of % total)',
            v_volume_id, v_primary, v_total;
    END IF;
END;
$$;


--
-- Name: revoke_sessions_on_credential_change(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.revoke_sessions_on_credential_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF (NEW.revoked_at IS NOT NULL AND OLD.revoked_at IS NULL)
       OR (NEW.secret_hash IS DISTINCT FROM OLD.secret_hash)
       OR (NEW.user_id IS DISTINCT FROM OLD.user_id)
       OR (NEW.kind IS DISTINCT FROM OLD.kind) THEN
        UPDATE sessions
        SET revoked_at = COALESCE(revoked_at, COALESCE(NEW.revoked_at, NOW()))
        WHERE credential_id = NEW.id
          AND revoked_at IS NULL;
    END IF;
    RETURN NEW;
END;
$$;


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
-- Name: trg_check_library_roots_integrity(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trg_check_library_roots_integrity() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_root_lib TEXT;
BEGIN
    SELECT library_id INTO v_root_lib FROM library_roots WHERE id = NEW.id FOR SHARE;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    PERFORM 1
    FROM media_assets a
    JOIN volumes v ON v.id = a.volume_id
    WHERE a.library_root_id = NEW.id
    FOR SHARE OF a, v;

    IF EXISTS (
        SELECT 1
        FROM media_assets a
        JOIN volumes v ON v.id = a.volume_id
        WHERE a.library_root_id = NEW.id
          AND v.library_id IS DISTINCT FROM v_root_lib
    ) THEN
        RAISE EXCEPTION 'library_root % library % conflicts with media_asset volume library', NEW.id, v_root_lib;
    END IF;

    RETURN NEW;
END;
$$;


--
-- Name: trg_check_media_assets_integrity(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trg_check_media_assets_integrity() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_vol_id     TEXT;
    v_root_id    TEXT;
    v_vol_lib    TEXT;
    v_root_lib   TEXT;
    v_pref_asset TEXT;
BEGIN
    SELECT volume_id, library_root_id INTO v_vol_id, v_root_id
    FROM media_assets WHERE id = NEW.id FOR SHARE;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    -- Bump parent tuple versions (without changing values or firing library_id/preferred_asset_id
    -- triggers) so concurrent REPEATABLE READ root/volume library updates serialize safely.
    UPDATE library_roots SET state = state WHERE id = v_root_id RETURNING library_id INTO v_root_lib;
    UPDATE volumes SET updated_at = updated_at WHERE id = v_vol_id RETURNING library_id INTO v_vol_lib;
    IF v_vol_lib IS DISTINCT FROM v_root_lib THEN
        RAISE EXCEPTION 'media_asset % volume % (library %) and library_root % (library %) must belong to the same library',
            NEW.id, v_vol_id, v_vol_lib, v_root_id, v_root_lib;
    END IF;

    IF TG_OP = 'UPDATE' AND OLD.volume_id IS DISTINCT FROM v_vol_id THEN
        UPDATE volumes SET updated_at = updated_at WHERE id = OLD.volume_id RETURNING preferred_asset_id INTO v_pref_asset;
        IF FOUND AND v_pref_asset = OLD.id THEN
            RAISE EXCEPTION 'cannot reassign asset % from volume % while it is preferred_asset_id',
                OLD.id, OLD.volume_id;
        END IF;
    END IF;

    RETURN NEW;
END;
$$;


--
-- Name: trg_check_series_integrity(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trg_check_series_integrity() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_ser_lib   TEXT;
    v_cover_vol TEXT;
BEGIN
    SELECT library_id, cover_volume_id INTO v_ser_lib, v_cover_vol
    FROM series WHERE id = NEW.id FOR SHARE;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    IF v_cover_vol IS NOT NULL THEN
        PERFORM 1
        FROM series_memberships
        WHERE series_id = NEW.id
          AND volume_id = v_cover_vol
        FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'series % cover_volume_id % must be a member of that series', NEW.id, v_cover_vol;
        END IF;
    END IF;

    PERFORM 1
    FROM series_memberships m
    JOIN volumes v ON v.id = m.volume_id
    WHERE m.series_id = NEW.id
    FOR SHARE OF m, v;

    IF EXISTS (
        SELECT 1
        FROM series_memberships m
        JOIN volumes v ON v.id = m.volume_id
        WHERE m.series_id = NEW.id
          AND v.library_id IS DISTINCT FROM v_ser_lib
    ) THEN
        RAISE EXCEPTION 'series % library % conflicts with member volume library', NEW.id, v_ser_lib;
    END IF;

    RETURN NEW;
END;
$$;


--
-- Name: trg_check_series_memberships_integrity(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trg_check_series_memberships_integrity() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_vol_lib   TEXT;
    v_ser_lib   TEXT;
    v_cover_vol TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM check_volume_primary_membership(NEW.volume_id);

        IF EXISTS (SELECT 1 FROM series_memberships WHERE volume_id = NEW.volume_id AND series_id = NEW.series_id) THEN
            SELECT library_id INTO v_vol_lib FROM volumes WHERE id = NEW.volume_id FOR SHARE;
            UPDATE series SET updated_at = updated_at WHERE id = NEW.series_id RETURNING library_id INTO v_ser_lib;
            IF v_vol_lib IS DISTINCT FROM v_ser_lib THEN
                RAISE EXCEPTION 'series_memberships volume % (library %) and series % (library %) must belong to the same library',
                    NEW.volume_id, v_vol_lib, NEW.series_id, v_ser_lib;
            END IF;
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.volume_id IS DISTINCT FROM NEW.volume_id THEN
            IF OLD.volume_id < NEW.volume_id THEN
                PERFORM check_volume_primary_membership(OLD.volume_id);
                PERFORM check_volume_primary_membership(NEW.volume_id);
            ELSE
                PERFORM check_volume_primary_membership(NEW.volume_id);
                PERFORM check_volume_primary_membership(OLD.volume_id);
            END IF;
        ELSE
            PERFORM check_volume_primary_membership(NEW.volume_id);
        END IF;

        IF EXISTS (SELECT 1 FROM series_memberships WHERE volume_id = NEW.volume_id AND series_id = NEW.series_id) THEN
            SELECT library_id INTO v_vol_lib FROM volumes WHERE id = NEW.volume_id FOR SHARE;
            UPDATE series SET updated_at = updated_at WHERE id = NEW.series_id RETURNING library_id INTO v_ser_lib;
            IF v_vol_lib IS DISTINCT FROM v_ser_lib THEN
                RAISE EXCEPTION 'series_memberships volume % (library %) and series % (library %) must belong to the same library',
                    NEW.volume_id, v_vol_lib, NEW.series_id, v_ser_lib;
            END IF;
        END IF;

        IF OLD.series_id IS DISTINCT FROM NEW.series_id OR OLD.volume_id IS DISTINCT FROM NEW.volume_id THEN
            UPDATE series SET updated_at = updated_at WHERE id = OLD.series_id RETURNING cover_volume_id INTO v_cover_vol;
            IF FOUND AND v_cover_vol = OLD.volume_id THEN
                IF NOT EXISTS (SELECT 1 FROM series_memberships WHERE series_id = OLD.series_id AND volume_id = OLD.volume_id) THEN
                    RAISE EXCEPTION 'cannot move membership (%, %) while series % references volume as cover_volume_id',
                        OLD.volume_id, OLD.series_id, OLD.series_id;
                END IF;
            END IF;
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM check_volume_primary_membership(OLD.volume_id);

        UPDATE series SET updated_at = updated_at WHERE id = OLD.series_id RETURNING cover_volume_id INTO v_cover_vol;
        IF FOUND AND v_cover_vol = OLD.volume_id THEN
            IF NOT EXISTS (SELECT 1 FROM series_memberships WHERE series_id = OLD.series_id AND volume_id = OLD.volume_id) THEN
                RAISE EXCEPTION 'cannot delete membership (%, %) while series % references volume as cover_volume_id',
                    OLD.volume_id, OLD.series_id, OLD.series_id;
            END IF;
        END IF;
        RETURN OLD;
    END IF;
    RETURN NULL;
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
    UPDATE labels
    SET built_in = built_in
    WHERE id = NEW.label_id
    RETURNING scope INTO v_scope;

    IF v_scope IS DISTINCT FROM 'SERVER' THEN
        RAISE EXCEPTION 'user_label_policies only permit SERVER labels, got % for label_id %', v_scope, NEW.label_id;
    END IF;
    RETURN NEW;
END;
$$;


--
-- Name: trg_check_volumes_integrity(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trg_check_volumes_integrity() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_vol_lib    TEXT;
    v_pref_asset TEXT;
    v_asset_vol  TEXT;
BEGIN
    SELECT library_id, preferred_asset_id INTO v_vol_lib, v_pref_asset
    FROM volumes WHERE id = NEW.id FOR SHARE;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    IF v_pref_asset IS NOT NULL THEN
        SELECT volume_id INTO v_asset_vol
        FROM media_assets
        WHERE id = v_pref_asset
        FOR SHARE;
        IF NOT FOUND OR v_asset_vol IS DISTINCT FROM NEW.id THEN
            RAISE EXCEPTION 'volume % preferred_asset_id % must belong to that volume (belongs to %)',
                NEW.id, v_pref_asset, v_asset_vol;
        END IF;
    END IF;

    PERFORM 1
    FROM series_memberships m
    JOIN series s ON s.id = m.series_id
    WHERE m.volume_id = NEW.id
    FOR SHARE OF m, s;

    IF EXISTS (
        SELECT 1
        FROM series_memberships m
        JOIN series s ON s.id = m.series_id
        WHERE m.volume_id = NEW.id
          AND s.library_id IS DISTINCT FROM v_vol_lib
    ) THEN
        RAISE EXCEPTION 'volume % library % conflicts with series membership library', NEW.id, v_vol_lib;
    END IF;

    PERFORM 1
    FROM media_assets a
    JOIN library_roots r ON r.id = a.library_root_id
    WHERE a.volume_id = NEW.id
    FOR SHARE OF a, r;

    IF EXISTS (
        SELECT 1
        FROM media_assets a
        JOIN library_roots r ON r.id = a.library_root_id
        WHERE a.volume_id = NEW.id
          AND r.library_id IS DISTINCT FROM v_vol_lib
    ) THEN
        RAISE EXCEPTION 'volume % library % conflicts with media_asset library_root library', NEW.id, v_vol_lib;
    END IF;

    RETURN NEW;
END;
$$;


--
-- Name: trg_protect_canonical_trash_label(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trg_protect_canonical_trash_label() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_is_canonical BOOLEAN;
BEGIN
    v_is_canonical := OLD.id = '019318a0-0000-7000-8000-000000000030'
        OR (OLD.scope = 'SERVER' AND OLD.name = 'marked_for_deletion');

    IF NOT v_is_canonical THEN
        RETURN COALESCE(NEW, OLD);
    END IF;

    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'cannot delete protected built-in trash label %', OLD.id;
    END IF;

    IF NEW.id IS DISTINCT FROM OLD.id
       OR NEW.scope IS DISTINCT FROM 'SERVER'
       OR NEW.owner_id IS NOT NULL
       OR NEW.name IS DISTINCT FROM 'marked_for_deletion'
       OR NEW.built_in IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION 'cannot modify protected built-in trash label %', OLD.id;
    END IF;

    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: asset_digests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.asset_digests (
    asset_id text NOT NULL,
    kind text NOT NULL,
    value text NOT NULL,
    is_current boolean DEFAULT true NOT NULL,
    reason text DEFAULT 'SCAN'::text NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL
);


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
-- Name: credits; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.credits (
    id text NOT NULL,
    person_id text NOT NULL,
    series_id text,
    volume_id text,
    role text NOT NULL,
    "position" integer DEFAULT 0 NOT NULL,
    locked_fields jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT credits_check CHECK (((series_id IS NULL) <> (volume_id IS NULL)))
);


--
-- Name: domain_event_sequence; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.domain_event_sequence (
    id boolean DEFAULT true NOT NULL,
    last_sequence bigint DEFAULT 0 NOT NULL,
    CONSTRAINT domain_event_sequence_id_check CHECK (id),
    CONSTRAINT domain_event_sequence_last_sequence_check CHECK ((last_sequence >= 0))
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
-- Name: entities; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.entities (
    id text NOT NULL,
    kind text NOT NULL,
    name text NOT NULL,
    aliases text[] DEFAULT '{}'::text[] NOT NULL,
    external_ids jsonb DEFAULT '{}'::jsonb NOT NULL,
    locked_fields jsonb DEFAULT '{}'::jsonb NOT NULL,
    import_key text,
    CONSTRAINT entities_import_key_nonempty CHECK (((import_key IS NULL) OR (import_key <> ''::text)))
);


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
-- Name: libraries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.libraries (
    id text NOT NULL,
    name text NOT NULL,
    mode text DEFAULT 'VIRTUAL'::text NOT NULL,
    intake_path_bytes bytea,
    intake_path_display text,
    intake_path_match text,
    auto_metadata_writeback boolean DEFAULT false NOT NULL,
    resolver_chain text[] DEFAULT '{metadata,folder,filename}'::text[] NOT NULL,
    metadata_precedence text DEFAULT 'EMBEDDED_OVER_ONLINE'::text NOT NULL,
    default_cover_policy text DEFAULT 'FIRST'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT libraries_check CHECK (((mode = 'MANAGED'::text) = (intake_path_bytes IS NOT NULL))),
    CONSTRAINT libraries_check1 CHECK (((intake_path_bytes IS NULL) = (intake_path_display IS NULL))),
    CONSTRAINT libraries_check2 CHECK (((intake_path_bytes IS NULL) = (intake_path_match IS NULL))),
    CONSTRAINT libraries_mode_check CHECK ((mode = ANY (ARRAY['VIRTUAL'::text, 'MANAGED'::text])))
);


--
-- Name: library_roots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.library_roots (
    id text NOT NULL,
    library_id text NOT NULL,
    path_bytes bytea NOT NULL,
    path_display text NOT NULL,
    path_match text NOT NULL,
    state text DEFAULT 'ONLINE'::text NOT NULL,
    last_checked_at timestamp with time zone,
    last_error text
);


--
-- Name: media_assets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.media_assets (
    id text NOT NULL,
    volume_id text NOT NULL,
    library_root_id text NOT NULL,
    rel_path_bytes bytea NOT NULL,
    rel_path_display text NOT NULL,
    rel_path_match text NOT NULL,
    file_size bigint NOT NULL,
    file_mtime timestamp with time zone,
    format text NOT NULL,
    page_count integer DEFAULT 0 NOT NULL,
    cover_page_index integer DEFAULT 0 NOT NULL,
    suppressed_pages integer[] DEFAULT '{}'::integer[] NOT NULL,
    diagnostics jsonb DEFAULT '{}'::jsonb NOT NULL,
    missing_since timestamp with time zone,
    retired_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT media_assets_check CHECK (((retired_at IS NULL) OR (missing_since IS NOT NULL)))
);


--
-- Name: people; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.people (
    id text NOT NULL,
    name text NOT NULL,
    name_native text,
    name_romanized text,
    aliases text[] DEFAULT '{}'::text[] NOT NULL,
    external_ids jsonb DEFAULT '{}'::jsonb NOT NULL,
    locked_fields jsonb DEFAULT '{}'::jsonb NOT NULL,
    import_key text,
    CONSTRAINT people_import_key_nonempty CHECK (((import_key IS NULL) OR (import_key <> ''::text)))
);


--
-- Name: reading_progress; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.reading_progress (
    user_id text NOT NULL,
    volume_id text NOT NULL,
    locator jsonb NOT NULL,
    total_progression real DEFAULT 0 NOT NULL,
    device text,
    source text NOT NULL,
    completed boolean DEFAULT false NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
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
-- Name: series; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.series (
    id text NOT NULL,
    library_id text NOT NULL,
    title text NOT NULL,
    title_sort text NOT NULL,
    language text,
    original_title text,
    romaji_title text,
    franchise text,
    edition_name text,
    publisher text,
    imprint text,
    status text DEFAULT 'ONGOING'::text NOT NULL,
    summary text,
    age_rating text,
    genres text[] DEFAULT '{}'::text[] NOT NULL,
    tags text[] DEFAULT '{}'::text[] NOT NULL,
    cover_policy text,
    cover_volume_id text,
    cover_image_digest text,
    locked_fields jsonb DEFAULT '{}'::jsonb NOT NULL,
    external_ids jsonb DEFAULT '{}'::jsonb NOT NULL,
    total_volumes integer,
    release_year integer,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    resolver_key text,
    CONSTRAINT series_check CHECK ((NOT ((cover_volume_id IS NOT NULL) AND (cover_image_digest IS NOT NULL)))),
    CONSTRAINT series_resolver_key_nonempty CHECK (((resolver_key IS NULL) OR (resolver_key <> ''::text)))
);


--
-- Name: series_memberships; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.series_memberships (
    volume_id text NOT NULL,
    series_id text NOT NULL,
    is_primary boolean DEFAULT false NOT NULL,
    kind text DEFAULT 'SERIES'::text NOT NULL,
    "position" double precision,
    source text DEFAULT 'RESOLVER'::text NOT NULL,
    locked_fields jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sessions (
    id text NOT NULL,
    user_id text NOT NULL,
    credential_id text NOT NULL,
    kind text NOT NULL,
    token_hash text NOT NULL,
    csrf_token_hash text NOT NULL,
    credential_hash_digest text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_used_at timestamp with time zone,
    expires_at timestamp with time zone NOT NULL,
    revoked_at timestamp with time zone,
    CONSTRAINT sessions_kind_check CHECK ((kind = ANY (ARRAY['NATIVE'::text, 'KOMGA'::text, 'KOMGA_REMEMBER_ME'::text])))
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
-- Name: volume_entities; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.volume_entities (
    volume_id text NOT NULL,
    entity_id text NOT NULL,
    locked_fields jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: volumes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.volumes (
    id text NOT NULL,
    library_id text NOT NULL,
    kind text DEFAULT 'VOLUME'::text NOT NULL,
    number double precision,
    chapter_start double precision,
    chapter_end double precision,
    chapter_range_source text,
    title text,
    summary text,
    release_date date,
    page_count integer DEFAULT 0 NOT NULL,
    preferred_asset_id text,
    external_ids jsonb DEFAULT '{}'::jsonb NOT NULL,
    locked_fields jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT volumes_check CHECK (((chapter_start IS NULL) = (chapter_end IS NULL))),
    CONSTRAINT volumes_check1 CHECK (((chapter_start IS NULL) = (chapter_range_source IS NULL))),
    CONSTRAINT volumes_check2 CHECK (((chapter_start IS NULL) OR (chapter_start <= chapter_end)))
);


--
-- Name: domain_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.domain_events ALTER COLUMN id SET DEFAULT nextval('public.domain_events_id_seq'::regclass);


--
-- Data for Name: asset_digests; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.asset_digests VALUES ('019318a0-0000-7000-8000-000000000098', 'XXH3_64_PARTIAL', '0123456789abcdef', true, 'SCAN', '2026-11-10 21:05:00+09');
INSERT INTO public.asset_digests VALUES ('019318a0-0000-7000-8000-000000000099', 'XXH3_64_PARTIAL', 'fedcba9876543210', true, 'SCAN', '2026-11-11 19:00:00+09');


--
-- Data for Name: credentials; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.credentials VALUES ('019318a0-0000-7000-8000-000000000020', '019318a0-0000-7000-8000-000000000010', 'PASSWORD', '{}', '$argon2id$v=19$m=65536,t=3,p=4$adminhash', 'argon2id', NULL, 'Admin password', '2026-11-10 21:00:00+09', NULL, NULL, NULL);
INSERT INTO public.credentials VALUES ('019318a0-0000-7000-8000-000000000021', '019318a0-0000-7000-8000-000000000010', 'PAT', '{read,progress:write,metadata:write,library:admin,admin}', '$argon2id$v=19$m=65536,t=3,p=4$patadminhash', 'argon2id', NULL, 'Admin PAT', '2026-11-10 21:05:00+09', NULL, NULL, NULL);
INSERT INTO public.credentials VALUES ('019318a0-0000-7000-8000-000000000022', '019318a0-0000-7000-8000-000000000011', 'PAT', '{read,progress:write}', '$argon2id$v=19$m=65536,t=3,p=4$patalicehash', 'argon2id', NULL, 'Mihon on Pixel', '2026-11-10 21:10:00+09', NULL, NULL, NULL);
INSERT INTO public.credentials VALUES ('019318a0-0000-7000-8000-000000000023', '019318a0-0000-7000-8000-000000000012', 'OIDC_LINK', '{}', NULL, NULL, 'https://auth.example.com|bob-subject-id', 'Authelia SSO', '2026-11-10 21:15:00+09', NULL, NULL, NULL);


--
-- Data for Name: credits; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.credits VALUES ('019318a0-0000-7000-8000-000000000201', '019318a0-0000-7000-8000-000000000101', '019318a0-0000-7000-8000-000000000090', NULL, 'AUTHOR', 0, '{"role": true}');
INSERT INTO public.credits VALUES ('019318a0-0000-7000-8000-000000000202', '019318a0-0000-7000-8000-000000000101', NULL, '019318a0-0000-7000-8000-000000000092', 'WRITER', 0, '{}');
INSERT INTO public.credits VALUES ('019318a0-0000-7000-8000-000000000203', '019318a0-0000-7000-8000-000000000102', NULL, '019318a0-0000-7000-8000-000000000093', 'PENCILLER', 0, '{}');
INSERT INTO public.credits VALUES ('019318a0-0000-7000-8000-000000000204', '019318a0-0000-7000-8000-000000000103', NULL, '019318a0-0000-7000-8000-000000000093', 'INKER', 1, '{}');
INSERT INTO public.credits VALUES ('019318a0-0000-7000-8000-000000000205', '019318a0-0000-7000-8000-000000000101', '019318a0-0000-7000-8000-000000000094', NULL, 'ARTIST', 0, '{}');


--
-- Data for Name: domain_event_sequence; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.domain_event_sequence VALUES (true, 1);


--
-- Data for Name: domain_events; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.domain_events VALUES (1, 1, '2026-11-10 21:00:00+09', 'user.created', 1, 'user', '019318a0-0000-7000-8000-000000000010', '{"username": "admin"}', '2026-11-10 21:00:00+09');
INSERT INTO public.domain_events VALUES (2, NULL, NULL, 'job.started', 1, 'job', '019318a0-0000-7000-8000-000000000040', '{"kind": "SCAN"}', '2026-11-10 21:30:00+09');


--
-- Data for Name: entities; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.entities VALUES ('019318a0-0000-7000-8000-000000000301', 'CHARACTER', 'Monkey D. Luffy', '{Luffy}', '{"anilist": 40}', '{"name": true}', NULL);
INSERT INTO public.entities VALUES ('019318a0-0000-7000-8000-000000000302', 'TEAM', 'Straw Hat Pirates', '{}', '{}', '{}', NULL);
INSERT INTO public.entities VALUES ('019318a0-0000-7000-8000-000000000303', 'LOCATION', 'Gotham', '{"Gotham City"}', '{}', '{}', NULL);
INSERT INTO public.entities VALUES ('019318a0-0000-7000-8000-000000000304', 'CHARACTER', 'Gotham', '{}', '{}', '{}', NULL);


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
INSERT INTO public.item_labels VALUES ('019318a0-0000-7000-8000-000000000033', 'SERIES', '019318a0-0000-7000-8000-000000000094');


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
INSERT INTO public.labels VALUES ('01a1fda4-2c5f-720e-b762-5c392f816cd9', 'USER', '019318a0-0000-7000-8000-000000000010', 'want-to-read', true);
INSERT INTO public.labels VALUES ('01a1fda4-2c5f-72d8-b0c8-4c93efcdeec9', 'USER', '019318a0-0000-7000-8000-000000000010', 'favorite', true);
INSERT INTO public.labels VALUES ('01a1fda4-2c5f-72ea-a13d-11279743d7a6', 'USER', '019318a0-0000-7000-8000-000000000010', 'dnf', true);
INSERT INTO public.labels VALUES ('019318a0-0000-7000-8000-000000000034', 'USER', '019318a0-0000-7000-8000-000000000011', 'want-to-read', true);
INSERT INTO public.labels VALUES ('019318a0-0000-7000-8000-000000000033', 'USER', '019318a0-0000-7000-8000-000000000011', 'favorite', true);
INSERT INTO public.labels VALUES ('01a1fda4-2c5f-73bf-81b1-5ca6f704f0d3', 'USER', '019318a0-0000-7000-8000-000000000011', 'dnf', true);
INSERT INTO public.labels VALUES ('01a1fda4-2c5f-73cb-9a2d-dec1e620dcae', 'USER', '019318a0-0000-7000-8000-000000000012', 'want-to-read', true);
INSERT INTO public.labels VALUES ('01a1fda4-2c5f-73d0-bf6d-cc590091488e', 'USER', '019318a0-0000-7000-8000-000000000012', 'favorite', true);
INSERT INTO public.labels VALUES ('01a1fda4-2c5f-73d9-8b2e-b703e72513f8', 'USER', '019318a0-0000-7000-8000-000000000012', 'dnf', true);


--
-- Data for Name: libraries; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.libraries VALUES ('019318a0-0000-7000-8000-000000000080', 'Manga', 'VIRTUAL', NULL, NULL, NULL, false, '{metadata,folder,filename}', 'EMBEDDED_OVER_ONLINE', 'FIRST', '2026-11-10 21:00:00+09');
INSERT INTO public.libraries VALUES ('019318a0-0000-7000-8000-000000000081', 'Comics', 'VIRTUAL', NULL, NULL, NULL, false, '{metadata,folder,filename}', 'EMBEDDED_OVER_ONLINE', 'FIRST', '2026-11-10 21:01:00+09');


--
-- Data for Name: library_roots; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.library_roots VALUES ('019318a0-0000-7000-8000-000000000086', '019318a0-0000-7000-8000-000000000081', '\x2f7372762f636f6d696373', '/srv/comics', '/srv/comics', 'ONLINE', NULL, NULL);
INSERT INTO public.library_roots VALUES ('019318a0-0000-7000-8000-000000000085', '019318a0-0000-7000-8000-000000000080', '\x2f7372762f6d616e6761', '/srv/manga', '/srv/manga', 'ONLINE', NULL, NULL);


--
-- Data for Name: media_assets; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.media_assets VALUES ('019318a0-0000-7000-8000-000000000098', '019318a0-0000-7000-8000-000000000092', '019318a0-0000-7000-8000-000000000085', '\x4f6e65205069656365207630312e63627a', 'One Piece v01.cbz', 'One Piece v01.cbz', 1048576, NULL, 'CBZ', 200, 0, '{}', '{}', NULL, NULL, '2026-11-10 21:05:00+09', '2026-11-10 21:05:00+09');
INSERT INTO public.media_assets VALUES ('019318a0-0000-7000-8000-000000000099', '019318a0-0000-7000-8000-000000000095', '019318a0-0000-7000-8000-000000000085', '\x446f726f6865646f726f207630312e63627a', 'Dorohedoro v01.cbz', 'Dorohedoro v01.cbz', 2097152, NULL, 'CBZ', 192, 0, '{}', '{}', NULL, NULL, '2026-11-11 19:00:00+09', '2026-11-11 19:00:00+09');


--
-- Data for Name: people; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.people VALUES ('019318a0-0000-7000-8000-000000000101', 'Eiichiro Oda', '尾田栄一郎', 'Oda Eiichiro', '{Oda}', '{"anilist": 1881}', '{"name": true, "name_native": true}', NULL);
INSERT INTO public.people VALUES ('019318a0-0000-7000-8000-000000000102', 'John Smith', NULL, NULL, '{"J. Smith"}', '{"comicvine": 100}', '{}', NULL);
INSERT INTO public.people VALUES ('019318a0-0000-7000-8000-000000000103', 'John Smith', NULL, NULL, '{}', '{"comicvine": 200}', '{"external_ids": true}', NULL);


--
-- Data for Name: reading_progress; Type: TABLE DATA; Schema: public; Owner: -
--



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
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000001', 'jobs:cancel_all');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000001', 'jobs:cancel_own');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000002', 'jobs:cancel_own');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000003', 'jobs:cancel_own');
INSERT INTO public.role_permissions VALUES ('019318a0-0000-7000-8000-000000000004', 'jobs:cancel_own');


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

INSERT INTO public.schema_migrations VALUES (1, '0001_init.sql', 'sha256:cf57a7fc15c3e5604c631a18a05139b22052d0ca876260ac1cf69cdeb6cea440', '2026-11-21 19:25:57.310752+09');
INSERT INTO public.schema_migrations VALUES (2, '0002_sessions.sql', 'sha256:86ee0a6b7b3583a9e4e5ebff8d16c4a88f287e69b54270d6fc73f85c1b3e3df2', '2026-11-21 19:25:57.328552+09');
INSERT INTO public.schema_migrations VALUES (3, '0003_session_credential_invalidation.sql', 'sha256:81836c37b82156f2267eaa71313507689ed659434ae63627c4d2db67d1b932ba', '2026-11-21 19:25:57.330442+09');
INSERT INTO public.schema_migrations VALUES (4, '0004_catalogue.sql', 'sha256:57fdfdf3d636cff6a83baa8ff46c6fda5cd6f6bdf897aad9ad88172e6de940ad', '2026-11-21 19:25:57.330689+09');
INSERT INTO public.schema_migrations VALUES (5, '0005_labels.sql', 'sha256:4db12cccb04c5568b1635097b01d67a7add200410bceee25f454c5affcccd4c3', '2026-11-21 19:25:57.342489+09');
INSERT INTO public.schema_migrations VALUES (6, '0006_domain_event_sequence.sql', 'sha256:6e5c3621f13deaa3faebdfa57576d194e0945ed90d08ab9fc4e1a0879ebe11db', '2026-11-21 19:25:57.343376+09');
INSERT INTO public.schema_migrations VALUES (7, '0007_job_cancel_permissions.sql', 'sha256:aa9641f8df91616db71c5b5699bf2294e1b80bdeff6e89753923ac46dc9415e5', '2026-11-21 19:25:57.344052+09');
INSERT INTO public.schema_migrations VALUES (8, '0008_people_entities_import_keys.sql', 'sha256:0643af58448a3bf03b568519bf5ad8a4c7b9a8a1258016a8b94c19b391954148', '2026-11-21 19:25:57.34436+09');
INSERT INTO public.schema_migrations VALUES (9, '0009_series_resolver_key.sql', 'sha256:21f0d8aa3f45ff34fc4519856efb9d6405d439f9cac1294733f34ac9795aad6f', '2026-11-21 19:25:57.345495+09');


--
-- Data for Name: series; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.series VALUES ('019318a0-0000-7000-8000-000000000090', '019318a0-0000-7000-8000-000000000080', 'One Piece', 'One Piece', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'ONGOING', NULL, NULL, '{}', '{}', NULL, NULL, NULL, '{}', '{}', NULL, NULL, '2026-11-10 21:05:00+09', '2026-11-10 21:05:00+09', NULL);
INSERT INTO public.series VALUES ('019318a0-0000-7000-8000-000000000091', '019318a0-0000-7000-8000-000000000081', 'Batman', 'Batman', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'ONGOING', NULL, NULL, '{}', '{}', NULL, NULL, NULL, '{}', '{}', NULL, NULL, '2026-11-10 21:06:00+09', '2026-11-10 21:06:00+09', NULL);
INSERT INTO public.series VALUES ('019318a0-0000-7000-8000-000000000094', '019318a0-0000-7000-8000-000000000080', 'Dorohedoro', 'Dorohedoro', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'COMPLETED', NULL, NULL, '{}', '{}', NULL, NULL, NULL, '{}', '{}', NULL, NULL, '2026-11-11 19:00:00+09', '2026-11-11 19:00:00+09', 'title:run=2018:dorohedoro');


--
-- Data for Name: series_memberships; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.series_memberships VALUES ('019318a0-0000-7000-8000-000000000092', '019318a0-0000-7000-8000-000000000090', true, 'SERIES', 1, 'RESOLVER', '{}');
INSERT INTO public.series_memberships VALUES ('019318a0-0000-7000-8000-000000000093', '019318a0-0000-7000-8000-000000000091', true, 'SERIES', 1, 'RESOLVER', '{}');
INSERT INTO public.series_memberships VALUES ('019318a0-0000-7000-8000-000000000095', '019318a0-0000-7000-8000-000000000094', true, 'SERIES', 1, 'RESOLVER', '{}');


--
-- Data for Name: sessions; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: system_settings; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.system_settings VALUES ('oidc.default_role_id', '"019318a0-0000-7000-8000-000000000003"', false, '2026-11-21 19:25:57.325111+09', NULL);
INSERT INTO public.system_settings VALUES ('backup.passphrase', '"Y2hhbmdlbWUtc2VjcmV0"', true, '2026-11-21 19:25:57.325111+09', NULL);
INSERT INTO public.system_settings VALUES ('events.retention_days', '7', false, '2026-11-21 19:25:57.325111+09', '019318a0-0000-7000-8000-000000000010');


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
-- Data for Name: volume_entities; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.volume_entities VALUES ('019318a0-0000-7000-8000-000000000092', '019318a0-0000-7000-8000-000000000301', '{"entity": true}');
INSERT INTO public.volume_entities VALUES ('019318a0-0000-7000-8000-000000000092', '019318a0-0000-7000-8000-000000000302', '{}');
INSERT INTO public.volume_entities VALUES ('019318a0-0000-7000-8000-000000000093', '019318a0-0000-7000-8000-000000000303', '{}');
INSERT INTO public.volume_entities VALUES ('019318a0-0000-7000-8000-000000000093', '019318a0-0000-7000-8000-000000000304', '{}');


--
-- Data for Name: volumes; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.volumes VALUES ('019318a0-0000-7000-8000-000000000093', '019318a0-0000-7000-8000-000000000081', 'ISSUE', 1, NULL, NULL, NULL, 'Year One', NULL, NULL, 32, NULL, '{}', '{}', '2026-11-10 21:06:00+09', '2026-11-10 21:06:00+09');
INSERT INTO public.volumes VALUES ('019318a0-0000-7000-8000-000000000092', '019318a0-0000-7000-8000-000000000080', 'VOLUME', 1, NULL, NULL, NULL, 'Romance Dawn', NULL, NULL, 200, NULL, '{}', '{}', '2026-11-10 21:05:00+09', '2026-11-10 21:05:00+09');
INSERT INTO public.volumes VALUES ('019318a0-0000-7000-8000-000000000095', '019318a0-0000-7000-8000-000000000080', 'VOLUME', 1, NULL, NULL, NULL, 'Dorohedoro v01', NULL, NULL, 192, NULL, '{}', '{}', '2026-11-11 19:00:00+09', '2026-11-11 19:00:00+09');


--
-- Name: domain_events_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.domain_events_id_seq', 2, true);


--
-- Name: asset_digests asset_digests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.asset_digests
    ADD CONSTRAINT asset_digests_pkey PRIMARY KEY (asset_id, kind, value);


--
-- Name: credentials credentials_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.credentials
    ADD CONSTRAINT credentials_pkey PRIMARY KEY (id);


--
-- Name: credits credits_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.credits
    ADD CONSTRAINT credits_pkey PRIMARY KEY (id);


--
-- Name: domain_event_sequence domain_event_sequence_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.domain_event_sequence
    ADD CONSTRAINT domain_event_sequence_pkey PRIMARY KEY (id);


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
-- Name: entities entities_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_pkey PRIMARY KEY (id);


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
-- Name: libraries libraries_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.libraries
    ADD CONSTRAINT libraries_name_key UNIQUE (name);


--
-- Name: libraries libraries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.libraries
    ADD CONSTRAINT libraries_pkey PRIMARY KEY (id);


--
-- Name: library_roots library_roots_path_bytes_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.library_roots
    ADD CONSTRAINT library_roots_path_bytes_key UNIQUE (path_bytes);


--
-- Name: library_roots library_roots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.library_roots
    ADD CONSTRAINT library_roots_pkey PRIMARY KEY (id);


--
-- Name: media_assets media_assets_library_root_id_rel_path_bytes_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_assets
    ADD CONSTRAINT media_assets_library_root_id_rel_path_bytes_key UNIQUE (library_root_id, rel_path_bytes);


--
-- Name: media_assets media_assets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_assets
    ADD CONSTRAINT media_assets_pkey PRIMARY KEY (id);


--
-- Name: people people_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.people
    ADD CONSTRAINT people_pkey PRIMARY KEY (id);


--
-- Name: reading_progress reading_progress_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reading_progress
    ADD CONSTRAINT reading_progress_pkey PRIMARY KEY (user_id, volume_id);


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
-- Name: series_memberships series_memberships_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_memberships
    ADD CONSTRAINT series_memberships_pkey PRIMARY KEY (volume_id, series_id);


--
-- Name: series series_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series
    ADD CONSTRAINT series_pkey PRIMARY KEY (id);


--
-- Name: sessions sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_pkey PRIMARY KEY (id);


--
-- Name: sessions sessions_token_hash_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_token_hash_key UNIQUE (token_hash);


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
-- Name: volume_entities volume_entities_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.volume_entities
    ADD CONSTRAINT volume_entities_pkey PRIMARY KEY (volume_id, entity_id);


--
-- Name: volumes volumes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.volumes
    ADD CONSTRAINT volumes_pkey PRIMARY KEY (id);


--
-- Name: idx_asset_digests_current_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_asset_digests_current_lookup ON public.asset_digests USING btree (kind, value) WHERE is_current;


--
-- Name: idx_asset_digests_history_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_asset_digests_history_lookup ON public.asset_digests USING btree (kind, value) WHERE (NOT is_current);


--
-- Name: idx_asset_digests_one_current; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_asset_digests_one_current ON public.asset_digests USING btree (asset_id, kind) WHERE is_current;


--
-- Name: idx_credentials_oidc; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_credentials_oidc ON public.credentials USING btree (external_subject) WHERE (kind = 'OIDC_LINK'::text);


--
-- Name: idx_credentials_password; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_credentials_password ON public.credentials USING btree (user_id) WHERE ((kind = 'PASSWORD'::text) AND (revoked_at IS NULL));


--
-- Name: idx_credits_person; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_credits_person ON public.credits USING btree (person_id);


--
-- Name: idx_credits_series; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_credits_series ON public.credits USING btree (series_id, person_id, role) WHERE (series_id IS NOT NULL);


--
-- Name: idx_credits_volume; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_credits_volume ON public.credits USING btree (volume_id, person_id, role) WHERE (volume_id IS NOT NULL);


--
-- Name: idx_domain_events_unpublished; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_domain_events_unpublished ON public.domain_events USING btree (id) WHERE (published_sequence IS NULL);


--
-- Name: idx_entities_import_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_entities_import_key ON public.entities USING btree (kind, import_key) WHERE (import_key IS NOT NULL);


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
-- Name: idx_media_assets_volume; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_media_assets_volume ON public.media_assets USING btree (volume_id);


--
-- Name: idx_memberships_one_primary; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_memberships_one_primary ON public.series_memberships USING btree (volume_id) WHERE is_primary;


--
-- Name: idx_people_import_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_people_import_key ON public.people USING btree (import_key) WHERE (import_key IS NOT NULL);


--
-- Name: idx_series_external_ids; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_series_external_ids ON public.series USING gin (external_ids jsonb_path_ops);


--
-- Name: idx_series_library_resolver_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_series_library_resolver_key ON public.series USING btree (library_id, resolver_key) WHERE (resolver_key IS NOT NULL);


--
-- Name: idx_series_memberships_series_primary; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_series_memberships_series_primary ON public.series_memberships USING btree (series_id) WHERE (is_primary = true);


--
-- Name: idx_series_romaji_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_series_romaji_trgm ON public.series USING gin (romaji_title public.gin_trgm_ops);


--
-- Name: idx_series_title_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_series_title_trgm ON public.series USING gin (title public.gin_trgm_ops);


--
-- Name: idx_sessions_credential_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sessions_credential_id ON public.sessions USING btree (credential_id);


--
-- Name: idx_sessions_expires_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sessions_expires_at ON public.sessions USING btree (expires_at);


--
-- Name: idx_sessions_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sessions_user_id ON public.sessions USING btree (user_id);


--
-- Name: idx_volume_entities_entity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_volume_entities_entity ON public.volume_entities USING btree (entity_id);


--
-- Name: idx_volumes_external_ids; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_volumes_external_ids ON public.volumes USING gin (external_ids jsonb_path_ops);


--
-- Name: credentials trg_credentials_revoke_sessions; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_credentials_revoke_sessions AFTER UPDATE ON public.credentials FOR EACH ROW EXECUTE FUNCTION public.revoke_sessions_on_credential_change();


--
-- Name: labels trg_labels_prevent_server_scope_change; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER trg_labels_prevent_server_scope_change AFTER UPDATE OF scope ON public.labels DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION public.trg_check_label_scope_update();


--
-- Name: labels trg_labels_protect_canonical_trash; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_labels_protect_canonical_trash BEFORE DELETE OR UPDATE ON public.labels FOR EACH ROW EXECUTE FUNCTION public.trg_protect_canonical_trash_label();


--
-- Name: library_roots trg_library_roots_integrity; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER trg_library_roots_integrity AFTER UPDATE OF library_id ON public.library_roots DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.trg_check_library_roots_integrity();


--
-- Name: media_assets trg_media_assets_integrity; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER trg_media_assets_integrity AFTER INSERT OR UPDATE OF volume_id, library_root_id ON public.media_assets DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.trg_check_media_assets_integrity();


--
-- Name: series trg_series_integrity; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER trg_series_integrity AFTER INSERT OR UPDATE OF library_id, cover_volume_id ON public.series DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.trg_check_series_integrity();


--
-- Name: series_memberships trg_series_memberships_integrity; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER trg_series_memberships_integrity AFTER INSERT OR DELETE OR UPDATE ON public.series_memberships DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.trg_check_series_memberships_integrity();


--
-- Name: user_label_policies trg_user_label_policies_server_only; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER trg_user_label_policies_server_only AFTER INSERT OR UPDATE ON public.user_label_policies DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION public.trg_check_user_label_policy_scope();


--
-- Name: volumes trg_volumes_integrity; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER trg_volumes_integrity AFTER INSERT OR UPDATE OF library_id, preferred_asset_id ON public.volumes DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.trg_check_volumes_integrity();


--
-- Name: asset_digests asset_digests_asset_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.asset_digests
    ADD CONSTRAINT asset_digests_asset_id_fkey FOREIGN KEY (asset_id) REFERENCES public.media_assets(id) ON DELETE CASCADE;


--
-- Name: credentials credentials_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.credentials
    ADD CONSTRAINT credentials_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: credits credits_person_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.credits
    ADD CONSTRAINT credits_person_id_fkey FOREIGN KEY (person_id) REFERENCES public.people(id) ON DELETE CASCADE;


--
-- Name: credits credits_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.credits
    ADD CONSTRAINT credits_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series(id) ON DELETE CASCADE;


--
-- Name: credits credits_volume_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.credits
    ADD CONSTRAINT credits_volume_id_fkey FOREIGN KEY (volume_id) REFERENCES public.volumes(id) ON DELETE CASCADE;


--
-- Name: series fk_series_cover_volume; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series
    ADD CONSTRAINT fk_series_cover_volume FOREIGN KEY (cover_volume_id) REFERENCES public.volumes(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;


--
-- Name: system_settings fk_system_settings_updated_by; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.system_settings
    ADD CONSTRAINT fk_system_settings_updated_by FOREIGN KEY (updated_by) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: volumes fk_volume_preferred_asset; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.volumes
    ADD CONSTRAINT fk_volume_preferred_asset FOREIGN KEY (preferred_asset_id) REFERENCES public.media_assets(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;


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
-- Name: library_roots library_roots_library_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.library_roots
    ADD CONSTRAINT library_roots_library_id_fkey FOREIGN KEY (library_id) REFERENCES public.libraries(id) ON DELETE CASCADE;


--
-- Name: media_assets media_assets_library_root_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_assets
    ADD CONSTRAINT media_assets_library_root_id_fkey FOREIGN KEY (library_root_id) REFERENCES public.library_roots(id);


--
-- Name: media_assets media_assets_volume_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_assets
    ADD CONSTRAINT media_assets_volume_id_fkey FOREIGN KEY (volume_id) REFERENCES public.volumes(id) ON DELETE CASCADE;


--
-- Name: reading_progress reading_progress_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reading_progress
    ADD CONSTRAINT reading_progress_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: reading_progress reading_progress_volume_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reading_progress
    ADD CONSTRAINT reading_progress_volume_id_fkey FOREIGN KEY (volume_id) REFERENCES public.volumes(id) ON DELETE CASCADE;


--
-- Name: role_permissions role_permissions_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.role_permissions
    ADD CONSTRAINT role_permissions_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.roles(id) ON DELETE CASCADE;


--
-- Name: series series_library_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series
    ADD CONSTRAINT series_library_id_fkey FOREIGN KEY (library_id) REFERENCES public.libraries(id) ON DELETE CASCADE;


--
-- Name: series_memberships series_memberships_series_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_memberships
    ADD CONSTRAINT series_memberships_series_id_fkey FOREIGN KEY (series_id) REFERENCES public.series(id) ON DELETE CASCADE;


--
-- Name: series_memberships series_memberships_volume_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_memberships
    ADD CONSTRAINT series_memberships_volume_id_fkey FOREIGN KEY (volume_id) REFERENCES public.volumes(id) ON DELETE CASCADE;


--
-- Name: sessions sessions_credential_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_credential_id_fkey FOREIGN KEY (credential_id) REFERENCES public.credentials(id) ON DELETE CASCADE;


--
-- Name: sessions sessions_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


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
-- Name: volume_entities volume_entities_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.volume_entities
    ADD CONSTRAINT volume_entities_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id) ON DELETE CASCADE;


--
-- Name: volume_entities volume_entities_volume_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.volume_entities
    ADD CONSTRAINT volume_entities_volume_id_fkey FOREIGN KEY (volume_id) REFERENCES public.volumes(id) ON DELETE CASCADE;


--
-- Name: volumes volumes_library_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.volumes
    ADD CONSTRAINT volumes_library_id_fkey FOREIGN KEY (library_id) REFERENCES public.libraries(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict JP4Kz2jKf3HSvWl8aBNfoFq6gwrkdNLOuvdeE3uJc4qN4ajhx4reX2efM8Px5II
