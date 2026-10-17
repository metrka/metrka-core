-- Canonical Metrka metadata schema for the first public 1.0 release.
-- Generated from the verified final pre-release Alembic head.

--
-- PostgreSQL database dump
--


-- Dumped from database version 18.6 (Debian 18.6-1.pgdg13+2)
-- Dumped by pg_dump version 18.6 (Debian 18.6-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: catalog; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA catalog;


--
-- Name: lineage; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA lineage;


--
-- Name: logs; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA logs;


--
-- Name: meta; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA meta;


--
-- Name: quality; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA quality;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: dataset_categories; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.dataset_categories (
    category_slug text NOT NULL,
    category_name text NOT NULL,
    description text,
    sort_order integer DEFAULT 100 NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: dataset_category_memberships; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.dataset_category_memberships (
    dataset_id text NOT NULL,
    category_slug text NOT NULL,
    source text DEFAULT 'contract'::text NOT NULL,
    contract_hash text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: dataset_publication_assets; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.dataset_publication_assets (
    publication_id text NOT NULL,
    file_path text NOT NULL,
    table_key text NOT NULL,
    file_format text NOT NULL,
    row_count bigint NOT NULL,
    column_count integer NOT NULL,
    columns_json jsonb NOT NULL,
    size_bytes bigint NOT NULL,
    checksum text NOT NULL,
    CONSTRAINT dataset_publication_assets_column_count_check CHECK ((column_count >= 0)),
    CONSTRAINT dataset_publication_assets_row_count_check CHECK ((row_count >= 0)),
    CONSTRAINT dataset_publication_assets_size_check CHECK ((size_bytes >= 0))
);


--
-- Name: dataset_publication_candidates; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.dataset_publication_candidates (
    candidate_id text NOT NULL,
    dataset_id text NOT NULL,
    version_period date NOT NULL,
    partition_key text NOT NULL,
    partition_value text NOT NULL,
    silver_build_id uuid NOT NULL,
    baseline_publication_id text,
    change_kind text NOT NULL,
    status text DEFAULT 'awaiting_approval'::text NOT NULL,
    fingerprint_version integer NOT NULL,
    logical_data_hash text NOT NULL,
    schema_hash text NOT NULL,
    requested_at timestamp with time zone NOT NULL,
    approved_at timestamp with time zone,
    approved_by text,
    rejected_at timestamp with time zone,
    rejected_by text,
    rejection_reason text,
    publication_id text,
    logical_hash_algorithm text NOT NULL,
    schema_hash_algorithm text NOT NULL,
    CONSTRAINT dataset_publication_candidates_logical_algorithm_check CHECK ((length(btrim(logical_hash_algorithm)) > 0)),
    CONSTRAINT dataset_publication_candidates_schema_algorithm_check CHECK ((length(btrim(schema_hash_algorithm)) > 0)),
    CONSTRAINT publication_candidates_baseline_check CHECK ((((change_kind = 'initial_publication'::text) AND (baseline_publication_id IS NULL)) OR ((change_kind <> 'initial_publication'::text) AND (baseline_publication_id IS NOT NULL)))),
    CONSTRAINT publication_candidates_change_kind_check CHECK ((change_kind = ANY (ARRAY['initial_publication'::text, 'fingerprint_version_changed'::text, 'fingerprint_algorithm_changed'::text, 'logical_data_changed'::text, 'schema_changed'::text, 'logical_data_and_schema_changed'::text]))),
    CONSTRAINT publication_candidates_lifecycle_check CHECK ((((status = 'awaiting_approval'::text) AND (approved_at IS NULL) AND (approved_by IS NULL) AND (rejected_at IS NULL) AND (rejected_by IS NULL) AND (rejection_reason IS NULL) AND (publication_id IS NULL)) OR ((status = 'approved'::text) AND (approved_at IS NOT NULL) AND (approved_by IS NOT NULL) AND (rejected_at IS NULL) AND (rejected_by IS NULL) AND (rejection_reason IS NULL) AND (publication_id IS NULL)) OR ((status = 'rejected'::text) AND (approved_at IS NULL) AND (approved_by IS NULL) AND (rejected_at IS NOT NULL) AND (rejected_by IS NOT NULL) AND (rejection_reason IS NOT NULL) AND (publication_id IS NULL)) OR ((status = 'published'::text) AND (approved_at IS NOT NULL) AND (approved_by IS NOT NULL) AND (rejected_at IS NULL) AND (rejected_by IS NULL) AND (rejection_reason IS NULL) AND (publication_id IS NOT NULL)))),
    CONSTRAINT publication_candidates_logical_hash_check CHECK ((length(logical_data_hash) = 64)),
    CONSTRAINT publication_candidates_schema_hash_check CHECK ((length(schema_hash) = 64)),
    CONSTRAINT publication_candidates_status_check CHECK ((status = ANY (ARRAY['awaiting_approval'::text, 'approved'::text, 'rejected'::text, 'published'::text]))),
    CONSTRAINT publication_candidates_version_check CHECK ((fingerprint_version > 0))
);


--
-- Name: dataset_publication_projection_states; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.dataset_publication_projection_states (
    dataset_id text NOT NULL,
    projection_kind text NOT NULL,
    expected_publication_id text CONSTRAINT dataset_publication_projection_expected_publication_id_not_null NOT NULL,
    projected_publication_id text,
    status text DEFAULT 'pending'::text NOT NULL,
    status_changed_at timestamp with time zone CONSTRAINT dataset_publication_projection_state_status_changed_at_not_null NOT NULL,
    last_attempted_at timestamp with time zone,
    last_synchronized_at timestamp with time zone,
    error jsonb,
    CONSTRAINT publication_projection_error_object_check CHECK (((error IS NULL) OR (jsonb_typeof(error) = 'object'::text))),
    CONSTRAINT publication_projection_kind_check CHECK ((projection_kind = ANY (ARRAY['current'::text, 'history'::text]))),
    CONSTRAINT publication_projection_pending_state_check CHECK (((status <> 'pending'::text) OR (error IS NULL))),
    CONSTRAINT publication_projection_stale_state_check CHECK (((status <> 'stale'::text) OR ((last_attempted_at IS NOT NULL) AND (error IS NOT NULL) AND (error <> '{}'::jsonb)))),
    CONSTRAINT publication_projection_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'synchronized'::text, 'stale'::text]))),
    CONSTRAINT publication_projection_synchronized_id_check CHECK (((status <> 'synchronized'::text) OR (projected_publication_id = expected_publication_id))),
    CONSTRAINT publication_projection_synchronized_state_check CHECK (((status <> 'synchronized'::text) OR ((last_attempted_at IS NOT NULL) AND (last_synchronized_at IS NOT NULL) AND (error IS NULL))))
);


--
-- Name: dataset_publications; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.dataset_publications (
    publication_id text NOT NULL,
    pipeline_run_id text NOT NULL,
    dataset_id text NOT NULL,
    version_period date NOT NULL,
    partition_key text NOT NULL,
    partition_value text NOT NULL,
    revision integer NOT NULL,
    silver_build_id uuid NOT NULL,
    manifest_path text NOT NULL,
    published_at timestamp with time zone NOT NULL,
    is_active_revision boolean DEFAULT true NOT NULL,
    is_current boolean DEFAULT false NOT NULL,
    supersedes_publication_id text,
    engine_release_id text NOT NULL,
    processing_config_hash text NOT NULL,
    quality_config_hash text NOT NULL,
    fingerprint_version integer NOT NULL,
    logical_data_hash text NOT NULL,
    schema_hash text NOT NULL,
    logical_hash_algorithm text NOT NULL,
    schema_hash_algorithm text NOT NULL,
    CONSTRAINT dataset_publications_current_is_active CHECK (((NOT is_current) OR is_active_revision)),
    CONSTRAINT dataset_publications_fingerprint_version_check CHECK ((fingerprint_version > 0)),
    CONSTRAINT dataset_publications_logical_algorithm_check CHECK ((length(btrim(logical_hash_algorithm)) > 0)),
    CONSTRAINT dataset_publications_logical_hash_check CHECK ((length(logical_data_hash) = 64)),
    CONSTRAINT dataset_publications_processing_hash_check CHECK ((length(processing_config_hash) = 64)),
    CONSTRAINT dataset_publications_quality_hash_check CHECK ((length(quality_config_hash) = 64)),
    CONSTRAINT dataset_publications_revision_check CHECK ((revision > 0)),
    CONSTRAINT dataset_publications_schema_algorithm_check CHECK ((length(btrim(schema_hash_algorithm)) > 0)),
    CONSTRAINT dataset_publications_schema_hash_check CHECK ((length(schema_hash) = 64))
);


--
-- Name: dataset_tag_memberships; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.dataset_tag_memberships (
    dataset_id text NOT NULL,
    tag_slug text NOT NULL,
    source text DEFAULT 'contract'::text NOT NULL,
    contract_hash text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    tag_position integer NOT NULL,
    tag_label text,
    CONSTRAINT dataset_tag_memberships_label_check CHECK (((tag_label IS NULL) OR (length(btrim(tag_label)) > 0))),
    CONSTRAINT dataset_tag_memberships_position_check CHECK ((tag_position > 0))
);


--
-- Name: dataset_tags; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.dataset_tags (
    tag_slug text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: transformation_impacts; Type: TABLE; Schema: lineage; Owner: -
--

CREATE TABLE lineage.transformation_impacts (
    transformation_impact_id text NOT NULL,
    recorded_at timestamp with time zone NOT NULL,
    pipeline_run_id text NOT NULL,
    dataset_id text NOT NULL,
    dataset_file_id text NOT NULL,
    bronze_run_id text NOT NULL,
    silver_run_id text NOT NULL,
    table_key text NOT NULL,
    operation text NOT NULL,
    column_name text NOT NULL,
    before_value jsonb NOT NULL,
    after_value jsonb NOT NULL,
    affected_row_count bigint NOT NULL,
    partition_key text,
    partition_value text,
    version_period date,
    contract_hash text,
    details_path text,
    details_hash text,
    details_row_count bigint,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    silver_build_id uuid,
    CONSTRAINT transformation_impacts_affected_rows_check CHECK ((affected_row_count >= 0)),
    CONSTRAINT transformation_impacts_details_rows_check CHECK (((details_row_count IS NULL) OR (details_row_count >= 0)))
);


--
-- Name: execution_logs; Type: TABLE; Schema: logs; Owner: -
--

CREATE TABLE logs.execution_logs (
    ts timestamp with time zone NOT NULL,
    schema_version integer NOT NULL,
    dataset text NOT NULL,
    dataset_id text,
    dataset_file_id uuid,
    source_file_name text,
    original_source_file_name text,
    table_key text,
    bronze_run_id text,
    silver_run_id text,
    partition_key text,
    partition_value text,
    version_period date,
    layer text NOT NULL,
    step text NOT NULL,
    run_id text NOT NULL,
    step_id text NOT NULL,
    event_type text NOT NULL,
    status text,
    duration_ms integer,
    success_count integer,
    failed_count integer,
    skipped_count integer,
    blocked_count integer,
    contract_hash text,
    contract_name text,
    contract_path text,
    contract_version text,
    contract_snapshot_yaml_path text,
    contract_snapshot_json_path text,
    input_row_count bigint,
    output_row_count bigint,
    input_column_count integer,
    output_column_count integer,
    input_file_count integer,
    output_file_count integer,
    input_byte_count bigint,
    output_byte_count bigint,
    manifest_path text,
    error_code text,
    error_message text,
    error jsonb,
    meta jsonb,
    pipeline_run_id text,
    silver_build_id uuid,
    execution_event_id bigint NOT NULL
);


--
-- Name: execution_logs_execution_event_id_seq; Type: SEQUENCE; Schema: logs; Owner: -
--

ALTER TABLE logs.execution_logs ALTER COLUMN execution_event_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME logs.execution_logs_execution_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: marshal_events; Type: TABLE; Schema: logs; Owner: -
--

CREATE TABLE logs.marshal_events (
    event_ts timestamp with time zone NOT NULL,
    event_type text NOT NULL,
    file_id text NOT NULL,
    reason text NOT NULL,
    stage text,
    bronze_run_id text,
    silver_run_id text,
    manifest_path text,
    landing_path text,
    partition_value text,
    old_entry jsonb,
    new_entry jsonb NOT NULL,
    meta jsonb NOT NULL,
    marshal_event_id bigint NOT NULL
);


--
-- Name: marshal_events_marshal_event_id_seq; Type: SEQUENCE; Schema: logs; Owner: -
--

ALTER TABLE logs.marshal_events ALTER COLUMN marshal_event_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME logs.marshal_events_marshal_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: pipeline_runs; Type: TABLE; Schema: logs; Owner: -
--

CREATE TABLE logs.pipeline_runs (
    pipeline_run_id text NOT NULL,
    workspace_name text NOT NULL,
    config_name text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    status text DEFAULT 'running'::text NOT NULL,
    code_provenance jsonb NOT NULL,
    error jsonb,
    source_capture_id text,
    CONSTRAINT pipeline_runs_status_check CHECK ((status = ANY (ARRAY['running'::text, 'success'::text, 'failed'::text])))
);


--
-- Name: silver_build_attempts; Type: TABLE; Schema: logs; Owner: -
--

CREATE TABLE logs.silver_build_attempts (
    silver_build_id uuid NOT NULL,
    pipeline_run_id text NOT NULL,
    silver_run_id text NOT NULL,
    dataset_file_id text NOT NULL,
    dataset_id text NOT NULL,
    version_period date,
    partition_key text,
    partition_value text,
    contract_hash text NOT NULL,
    engine_release_id text NOT NULL,
    processing_config_hash text NOT NULL,
    quality_config_hash text NOT NULL,
    build_signature text NOT NULL,
    fingerprint_version integer NOT NULL,
    status text NOT NULL,
    rebuild_mode text NOT NULL,
    rebuild_reasons jsonb DEFAULT '[]'::jsonb NOT NULL,
    started_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    error_code text,
    error_message text,
    logical_hash_algorithm text NOT NULL,
    schema_hash_algorithm text NOT NULL,
    CONSTRAINT silver_build_attempts_completed_at_check CHECK (((completed_at IS NULL) OR (completed_at >= started_at))),
    CONSTRAINT silver_build_attempts_contract_hash_check CHECK ((length(contract_hash) = 64)),
    CONSTRAINT silver_build_attempts_fingerprint_version_check CHECK ((fingerprint_version > 0)),
    CONSTRAINT silver_build_attempts_logical_algorithm_check CHECK ((length(btrim(logical_hash_algorithm)) > 0)),
    CONSTRAINT silver_build_attempts_processing_hash_check CHECK ((length(processing_config_hash) = 64)),
    CONSTRAINT silver_build_attempts_quality_hash_check CHECK ((length(quality_config_hash) = 64)),
    CONSTRAINT silver_build_attempts_rebuild_mode_check CHECK ((rebuild_mode = ANY (ARRAY['automatic'::text, 'manual'::text]))),
    CONSTRAINT silver_build_attempts_rebuild_reasons_check CHECK ((jsonb_typeof(rebuild_reasons) = 'array'::text)),
    CONSTRAINT silver_build_attempts_schema_algorithm_check CHECK ((length(btrim(schema_hash_algorithm)) > 0)),
    CONSTRAINT silver_build_attempts_signature_check CHECK ((length(build_signature) = 64)),
    CONSTRAINT silver_build_attempts_status_check CHECK ((status = ANY (ARRAY['running'::text, 'succeeded'::text, 'failed'::text])))
);


--
-- Name: contract_snapshots; Type: TABLE; Schema: meta; Owner: -
--

CREATE TABLE meta.contract_snapshots (
    contract_hash text CONSTRAINT contract_marshal_contract_hash_not_null NOT NULL,
    dataset text CONSTRAINT contract_marshal_dataset_not_null NOT NULL,
    dataset_id text NOT NULL,
    contract_name text CONSTRAINT contract_marshal_contract_name_not_null NOT NULL,
    contract_stem text CONSTRAINT contract_marshal_contract_stem_not_null NOT NULL,
    contract_path text CONSTRAINT contract_marshal_contract_path_not_null NOT NULL,
    contract_version text,
    snapshot_yaml_path text CONSTRAINT contract_marshal_snapshot_yaml_path_not_null NOT NULL,
    snapshot_json_path text,
    git_commit_sha text,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT contract_marshal_created_at_not_null NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb CONSTRAINT contract_marshal_meta_not_null NOT NULL
);


--
-- Name: marshaled_files; Type: TABLE; Schema: meta; Owner: -
--

CREATE TABLE meta.marshaled_files (
    dataset_file_id text NOT NULL,
    dataset_id text,
    source_url text,
    source_file_name text,
    source_hash text,
    file_size bigint,
    ingestion_timestamp timestamp with time zone,
    source_last_modified timestamp with time zone,
    row_count_raw integer,
    column_count_raw integer,
    bronze_run_id text,
    silver_run_id text,
    landing_path text,
    manifest_path text,
    partition_key text,
    partition_value text,
    is_promoted boolean,
    version_period date,
    promoted_at timestamp with time zone,
    superseded_by_file_id text,
    original_source_file_name text,
    artifact_role text DEFAULT 'data'::text NOT NULL,
    bronze_artifacts jsonb NOT NULL,
    CONSTRAINT marshaled_files_bronze_artifacts_array_check CHECK ((jsonb_typeof(bronze_artifacts) = 'array'::text))
);


--
-- Name: silver_engine_releases; Type: TABLE; Schema: meta; Owner: -
--

CREATE TABLE meta.silver_engine_releases (
    engine_release_id text NOT NULL,
    release_hash text NOT NULL,
    engine_hash text NOT NULL,
    engine_fingerprint_version integer NOT NULL,
    runtime_hash text NOT NULL,
    runtime_fingerprint_version integer NOT NULL,
    component_hashes jsonb NOT NULL,
    runtime_versions jsonb NOT NULL,
    core_commit_sha text NOT NULL,
    status text NOT NULL,
    detected_at timestamp with time zone NOT NULL,
    approved_at timestamp with time zone,
    approved_by text,
    rejected_at timestamp with time zone,
    rejected_by text,
    rejection_reason text,
    CONSTRAINT silver_engine_engine_hash_check CHECK ((length(engine_hash) = 64)),
    CONSTRAINT silver_engine_fingerprint_version_check CHECK ((engine_fingerprint_version > 0)),
    CONSTRAINT silver_engine_release_hash_check CHECK ((length(release_hash) = 64)),
    CONSTRAINT silver_engine_runtime_hash_check CHECK ((length(runtime_hash) = 64)),
    CONSTRAINT silver_engine_runtime_version_check CHECK ((runtime_fingerprint_version > 0)),
    CONSTRAINT silver_engine_status_check CHECK ((status = ANY (ARRAY['candidate'::text, 'approved'::text, 'rejected'::text, 'retired'::text])))
);


--
-- Name: silver_materializations; Type: TABLE; Schema: meta; Owner: -
--

CREATE TABLE meta.silver_materializations (
    silver_build_id uuid NOT NULL,
    manifest_path text NOT NULL,
    output_hash text,
    output_file_count integer NOT NULL,
    output_byte_count bigint NOT NULL,
    logical_data_hash text NOT NULL,
    schema_hash text NOT NULL,
    materialized_at timestamp with time zone NOT NULL,
    CONSTRAINT silver_materializations_logical_hash_check CHECK ((length(logical_data_hash) = 64)),
    CONSTRAINT silver_materializations_output_byte_count_check CHECK ((output_byte_count >= 0)),
    CONSTRAINT silver_materializations_output_file_count_check CHECK ((output_file_count > 0)),
    CONSTRAINT silver_materializations_output_hash_check CHECK (((output_hash IS NULL) OR (length(output_hash) = 64))),
    CONSTRAINT silver_materializations_schema_hash_check CHECK ((length(schema_hash) = 64))
);


--
-- Name: source_capture_assets; Type: TABLE; Schema: meta; Owner: -
--

CREATE TABLE meta.source_capture_assets (
    source_capture_id text NOT NULL,
    stream_name text NOT NULL,
    dataset_id text NOT NULL,
    dataset_file_id text NOT NULL,
    relative_path text NOT NULL,
    source_url text NOT NULL,
    artifact_role text NOT NULL,
    source_last_modified timestamp with time zone,
    bound_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT source_capture_assets_role_check CHECK ((artifact_role = ANY (ARRAY['data'::text, 'source_schema'::text, 'documentation'::text])))
);


--
-- Name: source_captures; Type: TABLE; Schema: meta; Owner: -
--

CREATE TABLE meta.source_captures (
    source_capture_id text NOT NULL,
    workspace_name text NOT NULL,
    captured_at timestamp with time zone NOT NULL,
    capture_path text NOT NULL,
    registered_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: source_schema_bindings; Type: TABLE; Schema: meta; Owner: -
--

CREATE TABLE meta.source_schema_bindings (
    schema_snapshot_id text NOT NULL,
    data_file_id text NOT NULL,
    bound_at timestamp with time zone DEFAULT now() NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: source_schema_fields; Type: TABLE; Schema: meta; Owner: -
--

CREATE TABLE meta.source_schema_fields (
    schema_snapshot_id text NOT NULL,
    table_name text NOT NULL,
    field_name text NOT NULL,
    ordinal_position integer NOT NULL,
    source_type text NOT NULL,
    source_length integer,
    nullable boolean,
    attributes jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT source_schema_fields_ordinal_position_check CHECK ((ordinal_position > 0)),
    CONSTRAINT source_schema_fields_source_length_check CHECK (((source_length IS NULL) OR (source_length > 0)))
);


--
-- Name: source_schema_snapshots; Type: TABLE; Schema: meta; Owner: -
--

CREATE TABLE meta.source_schema_snapshots (
    schema_snapshot_id text NOT NULL,
    source_schema_file_id text NOT NULL,
    dataset_id text NOT NULL,
    schema_hash text NOT NULL,
    source_format text NOT NULL,
    parser_name text NOT NULL,
    parser_version text NOT NULL,
    table_count integer NOT NULL,
    field_count integer NOT NULL,
    parsed_at timestamp with time zone DEFAULT now() NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    schema_hash_algorithm text NOT NULL,
    field_binding text NOT NULL,
    CONSTRAINT source_schema_snapshots_field_binding_check CHECK ((field_binding = ANY (ARRAY['by_name'::text, 'by_position'::text]))),
    CONSTRAINT source_schema_snapshots_field_count_check CHECK ((field_count >= 0)),
    CONSTRAINT source_schema_snapshots_hash_algorithm_check CHECK ((length(btrim(schema_hash_algorithm)) > 0)),
    CONSTRAINT source_schema_snapshots_table_count_check CHECK ((table_count >= 0))
);


--
-- Name: asset_integrity_batches; Type: TABLE; Schema: quality; Owner: -
--

CREATE TABLE quality.asset_integrity_batches (
    integrity_batch_id bigint NOT NULL,
    checked_at timestamp with time zone NOT NULL
);


--
-- Name: asset_integrity_batches_integrity_batch_id_seq; Type: SEQUENCE; Schema: quality; Owner: -
--

ALTER TABLE quality.asset_integrity_batches ALTER COLUMN integrity_batch_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME quality.asset_integrity_batches_integrity_batch_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: asset_integrity_results; Type: TABLE; Schema: quality; Owner: -
--

CREATE TABLE quality.asset_integrity_results (
    integrity_batch_id bigint NOT NULL,
    file_path text NOT NULL,
    status text NOT NULL,
    expected_size_bytes bigint NOT NULL,
    actual_size_bytes bigint,
    expected_checksum text NOT NULL,
    actual_checksum text,
    failure_codes jsonb NOT NULL,
    error_type text,
    error_message text,
    CONSTRAINT asset_integrity_results_error_pair_check CHECK (((error_type IS NULL) = (error_message IS NULL))),
    CONSTRAINT asset_integrity_results_failures_array_check CHECK ((jsonb_typeof(failure_codes) = 'array'::text)),
    CONSTRAINT asset_integrity_results_result_check CHECK ((((status = 'passed'::text) AND (jsonb_array_length(failure_codes) = 0) AND (actual_size_bytes IS NOT NULL) AND (actual_checksum IS NOT NULL) AND (error_type IS NULL) AND (error_message IS NULL)) OR ((status = 'failed'::text) AND (jsonb_array_length(failure_codes) > 0)))),
    CONSTRAINT asset_integrity_results_size_check CHECK (((expected_size_bytes >= 0) AND ((actual_size_bytes IS NULL) OR (actual_size_bytes >= 0)))),
    CONSTRAINT asset_integrity_results_status_check CHECK ((status = ANY (ARRAY['passed'::text, 'failed'::text])))
);


--
-- Name: publication_gate_attempts; Type: TABLE; Schema: quality; Owner: -
--

CREATE TABLE quality.publication_gate_attempts (
    gate_attempt_id bigint NOT NULL,
    candidate_id text NOT NULL,
    silver_build_id uuid NOT NULL,
    pipeline_run_id text NOT NULL,
    integrity_batch_id bigint NOT NULL
);


--
-- Name: publication_gate_attempts_gate_attempt_id_seq; Type: SEQUENCE; Schema: quality; Owner: -
--

ALTER TABLE quality.publication_gate_attempts ALTER COLUMN gate_attempt_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME quality.publication_gate_attempts_gate_attempt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: publication_integrity_checks; Type: TABLE; Schema: quality; Owner: -
--

CREATE TABLE quality.publication_integrity_checks (
    publication_id text NOT NULL,
    integrity_batch_id bigint NOT NULL,
    verification_trigger text NOT NULL,
    CONSTRAINT publication_integrity_checks_trigger_check CHECK ((verification_trigger = ANY (ARRAY['publication_commit'::text, 'reconciliation'::text])))
);


--
-- Name: quality_check_definitions; Type: TABLE; Schema: quality; Owner: -
--

CREATE TABLE quality.quality_check_definitions (
    check_id text NOT NULL,
    check_name text NOT NULL,
    check_type text NOT NULL,
    layer text NOT NULL,
    target text NOT NULL,
    severity text NOT NULL,
    description text,
    code_ref text NOT NULL,
    default_params jsonb DEFAULT '{}'::jsonb NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: quality_check_runs; Type: TABLE; Schema: quality; Owner: -
--

CREATE TABLE quality.quality_check_runs (
    check_run_id uuid DEFAULT gen_random_uuid() NOT NULL,
    check_id text NOT NULL,
    check_name text NOT NULL,
    check_type text NOT NULL,
    dataset_id text,
    dataset_file_id text,
    run_id text,
    step_id text,
    layer text NOT NULL,
    target text NOT NULL,
    severity text NOT NULL,
    status text NOT NULL,
    expected jsonb,
    actual jsonb,
    result_summary text,
    details jsonb,
    code_ref text NOT NULL,
    params jsonb,
    duration_ms integer,
    executed_at timestamp with time zone DEFAULT now() NOT NULL,
    pipeline_run_id text,
    silver_build_id uuid
);


--
-- Name: silver_publication_verifications; Type: TABLE; Schema: quality; Owner: -
--

CREATE TABLE quality.silver_publication_verifications (
    publication_id text NOT NULL,
    engine_hash text NOT NULL,
    logical_hash_algorithm text CONSTRAINT silver_publication_verification_logical_hash_algorithm_not_null NOT NULL,
    latest_silver_build_id uuid CONSTRAINT silver_publication_verification_latest_silver_build_id_not_null NOT NULL,
    quality_config_hash text NOT NULL,
    verification_count bigint DEFAULT 1 NOT NULL,
    first_verified_at timestamp with time zone NOT NULL,
    last_verified_at timestamp with time zone NOT NULL,
    schema_hash_algorithm text NOT NULL,
    CONSTRAINT silver_publication_verifications_schema_algorithm_check CHECK ((length(btrim(schema_hash_algorithm)) > 0)),
    CONSTRAINT silver_verifications_algorithm_check CHECK ((btrim(logical_hash_algorithm) <> ''::text)),
    CONSTRAINT silver_verifications_count_check CHECK ((verification_count > 0)),
    CONSTRAINT silver_verifications_engine_hash_check CHECK ((length(engine_hash) = 64)),
    CONSTRAINT silver_verifications_quality_hash_check CHECK ((length(quality_config_hash) = 64)),
    CONSTRAINT silver_verifications_time_check CHECK ((last_verified_at >= first_verified_at))
);


--
-- Name: dataset_categories dataset_categories_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_categories
    ADD CONSTRAINT dataset_categories_pkey PRIMARY KEY (category_slug);


--
-- Name: dataset_category_memberships dataset_category_memberships_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_category_memberships
    ADD CONSTRAINT dataset_category_memberships_pkey PRIMARY KEY (dataset_id, category_slug);


--
-- Name: dataset_publication_assets dataset_publication_assets_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publication_assets
    ADD CONSTRAINT dataset_publication_assets_pkey PRIMARY KEY (publication_id, file_path);


--
-- Name: dataset_publication_candidates dataset_publication_candidates_build_unique; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publication_candidates
    ADD CONSTRAINT dataset_publication_candidates_build_unique UNIQUE (silver_build_id);


--
-- Name: dataset_publication_candidates dataset_publication_candidates_identity_unique; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publication_candidates
    ADD CONSTRAINT dataset_publication_candidates_identity_unique UNIQUE (candidate_id, silver_build_id);


--
-- Name: dataset_publication_candidates dataset_publication_candidates_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publication_candidates
    ADD CONSTRAINT dataset_publication_candidates_pkey PRIMARY KEY (candidate_id);


--
-- Name: dataset_publication_projection_states dataset_publication_projection_states_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publication_projection_states
    ADD CONSTRAINT dataset_publication_projection_states_pkey PRIMARY KEY (dataset_id, projection_kind);


--
-- Name: dataset_publications dataset_publications_build_unique; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publications
    ADD CONSTRAINT dataset_publications_build_unique UNIQUE (silver_build_id);


--
-- Name: dataset_publications dataset_publications_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publications
    ADD CONSTRAINT dataset_publications_pkey PRIMARY KEY (publication_id);


--
-- Name: dataset_publications dataset_publications_revision_unique; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publications
    ADD CONSTRAINT dataset_publications_revision_unique UNIQUE (dataset_id, partition_value, revision);


--
-- Name: dataset_tag_memberships dataset_tag_memberships_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_tag_memberships
    ADD CONSTRAINT dataset_tag_memberships_pkey PRIMARY KEY (dataset_id, tag_slug);


--
-- Name: dataset_tag_memberships dataset_tag_memberships_position_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_tag_memberships
    ADD CONSTRAINT dataset_tag_memberships_position_key UNIQUE (dataset_id, tag_position);


--
-- Name: dataset_tags dataset_tags_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_tags
    ADD CONSTRAINT dataset_tags_pkey PRIMARY KEY (tag_slug);


--
-- Name: transformation_impacts transformation_impacts_pkey; Type: CONSTRAINT; Schema: lineage; Owner: -
--

ALTER TABLE ONLY lineage.transformation_impacts
    ADD CONSTRAINT transformation_impacts_pkey PRIMARY KEY (transformation_impact_id);


--
-- Name: execution_logs execution_logs_pkey; Type: CONSTRAINT; Schema: logs; Owner: -
--

ALTER TABLE ONLY logs.execution_logs
    ADD CONSTRAINT execution_logs_pkey PRIMARY KEY (execution_event_id);


--
-- Name: execution_logs execution_logs_run_step_event_key; Type: CONSTRAINT; Schema: logs; Owner: -
--

ALTER TABLE ONLY logs.execution_logs
    ADD CONSTRAINT execution_logs_run_step_event_key UNIQUE (run_id, step_id, event_type);


--
-- Name: marshal_events marshal_events_pkey; Type: CONSTRAINT; Schema: logs; Owner: -
--

ALTER TABLE ONLY logs.marshal_events
    ADD CONSTRAINT marshal_events_pkey PRIMARY KEY (marshal_event_id);


--
-- Name: pipeline_runs pipeline_runs_pkey; Type: CONSTRAINT; Schema: logs; Owner: -
--

ALTER TABLE ONLY logs.pipeline_runs
    ADD CONSTRAINT pipeline_runs_pkey PRIMARY KEY (pipeline_run_id);


--
-- Name: silver_build_attempts silver_build_attempts_pkey; Type: CONSTRAINT; Schema: logs; Owner: -
--

ALTER TABLE ONLY logs.silver_build_attempts
    ADD CONSTRAINT silver_build_attempts_pkey PRIMARY KEY (silver_build_id);


--
-- Name: contract_snapshots contract_snapshots_pkey; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.contract_snapshots
    ADD CONSTRAINT contract_snapshots_pkey PRIMARY KEY (dataset_id, contract_hash);


--
-- Name: marshaled_files marshaled_files_pkey; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.marshaled_files
    ADD CONSTRAINT marshaled_files_pkey PRIMARY KEY (dataset_file_id);


--
-- Name: silver_engine_releases silver_engine_releases_hash_unique; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.silver_engine_releases
    ADD CONSTRAINT silver_engine_releases_hash_unique UNIQUE (release_hash);


--
-- Name: silver_engine_releases silver_engine_releases_pkey; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.silver_engine_releases
    ADD CONSTRAINT silver_engine_releases_pkey PRIMARY KEY (engine_release_id);


--
-- Name: silver_materializations silver_materializations_pkey; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.silver_materializations
    ADD CONSTRAINT silver_materializations_pkey PRIMARY KEY (silver_build_id);


--
-- Name: source_capture_assets source_capture_assets_pkey; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_capture_assets
    ADD CONSTRAINT source_capture_assets_pkey PRIMARY KEY (source_capture_id, stream_name, relative_path);


--
-- Name: source_captures source_captures_pkey; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_captures
    ADD CONSTRAINT source_captures_pkey PRIMARY KEY (source_capture_id);


--
-- Name: source_schema_bindings source_schema_bindings_pkey; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_schema_bindings
    ADD CONSTRAINT source_schema_bindings_pkey PRIMARY KEY (schema_snapshot_id, data_file_id);


--
-- Name: source_schema_fields source_schema_fields_pkey; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_schema_fields
    ADD CONSTRAINT source_schema_fields_pkey PRIMARY KEY (schema_snapshot_id, table_name, field_name);


--
-- Name: source_schema_fields source_schema_fields_schema_snapshot_id_table_name_ordinal__key; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_schema_fields
    ADD CONSTRAINT source_schema_fields_schema_snapshot_id_table_name_ordinal__key UNIQUE (schema_snapshot_id, table_name, ordinal_position);


--
-- Name: source_schema_snapshots source_schema_snapshots_pkey; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_schema_snapshots
    ADD CONSTRAINT source_schema_snapshots_pkey PRIMARY KEY (schema_snapshot_id);


--
-- Name: source_schema_snapshots source_schema_snapshots_source_schema_file_id_parser_name_p_key; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_schema_snapshots
    ADD CONSTRAINT source_schema_snapshots_source_schema_file_id_parser_name_p_key UNIQUE (source_schema_file_id, parser_name, parser_version, schema_hash_algorithm);


--
-- Name: source_capture_assets uq_source_capture_assets_path; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_capture_assets
    ADD CONSTRAINT uq_source_capture_assets_path UNIQUE (source_capture_id, relative_path);


--
-- Name: source_captures uq_source_captures_workspace_path; Type: CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_captures
    ADD CONSTRAINT uq_source_captures_workspace_path UNIQUE (workspace_name, capture_path);


--
-- Name: asset_integrity_batches asset_integrity_batches_pkey; Type: CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.asset_integrity_batches
    ADD CONSTRAINT asset_integrity_batches_pkey PRIMARY KEY (integrity_batch_id);


--
-- Name: asset_integrity_results asset_integrity_results_pkey; Type: CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.asset_integrity_results
    ADD CONSTRAINT asset_integrity_results_pkey PRIMARY KEY (integrity_batch_id, file_path);


--
-- Name: publication_gate_attempts publication_gate_attempts_integrity_batch_unique; Type: CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.publication_gate_attempts
    ADD CONSTRAINT publication_gate_attempts_integrity_batch_unique UNIQUE (integrity_batch_id);


--
-- Name: publication_gate_attempts publication_gate_attempts_pkey; Type: CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.publication_gate_attempts
    ADD CONSTRAINT publication_gate_attempts_pkey PRIMARY KEY (gate_attempt_id);


--
-- Name: publication_integrity_checks publication_integrity_checks_batch_unique; Type: CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.publication_integrity_checks
    ADD CONSTRAINT publication_integrity_checks_batch_unique UNIQUE (integrity_batch_id);


--
-- Name: publication_integrity_checks publication_integrity_checks_pkey; Type: CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.publication_integrity_checks
    ADD CONSTRAINT publication_integrity_checks_pkey PRIMARY KEY (publication_id, integrity_batch_id);


--
-- Name: quality_check_definitions quality_check_definitions_pkey; Type: CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.quality_check_definitions
    ADD CONSTRAINT quality_check_definitions_pkey PRIMARY KEY (check_id);


--
-- Name: quality_check_runs quality_check_runs_pkey; Type: CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.quality_check_runs
    ADD CONSTRAINT quality_check_runs_pkey PRIMARY KEY (check_run_id);


--
-- Name: silver_publication_verifications silver_publication_verifications_pkey; Type: CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.silver_publication_verifications
    ADD CONSTRAINT silver_publication_verifications_pkey PRIMARY KEY (publication_id, engine_hash, logical_hash_algorithm, schema_hash_algorithm);


--
-- Name: idx_dataset_category_memberships_category; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX idx_dataset_category_memberships_category ON catalog.dataset_category_memberships USING btree (category_slug, dataset_id);


--
-- Name: idx_dataset_publication_assets_table; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX idx_dataset_publication_assets_table ON catalog.dataset_publication_assets USING btree (publication_id, table_key);


--
-- Name: idx_dataset_publication_projection_states_status; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX idx_dataset_publication_projection_states_status ON catalog.dataset_publication_projection_states USING btree (status, dataset_id);


--
-- Name: idx_dataset_publications_dataset_version; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX idx_dataset_publications_dataset_version ON catalog.dataset_publications USING btree (dataset_id, version_period DESC, revision DESC);


--
-- Name: idx_dataset_tag_memberships_tag; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX idx_dataset_tag_memberships_tag ON catalog.dataset_tag_memberships USING btree (tag_slug, dataset_id);


--
-- Name: idx_publication_candidates_baseline; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX idx_publication_candidates_baseline ON catalog.dataset_publication_candidates USING btree (baseline_publication_id);


--
-- Name: idx_publication_candidates_dataset; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX idx_publication_candidates_dataset ON catalog.dataset_publication_candidates USING btree (dataset_id, version_period);


--
-- Name: idx_publication_candidates_status; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX idx_publication_candidates_status ON catalog.dataset_publication_candidates USING btree (status, requested_at);


--
-- Name: uq_dataset_publications_active_revision; Type: INDEX; Schema: catalog; Owner: -
--

CREATE UNIQUE INDEX uq_dataset_publications_active_revision ON catalog.dataset_publications USING btree (dataset_id, partition_value) WHERE is_active_revision;


--
-- Name: uq_dataset_publications_current; Type: INDEX; Schema: catalog; Owner: -
--

CREATE UNIQUE INDEX uq_dataset_publications_current ON catalog.dataset_publications USING btree (dataset_id) WHERE is_current;


--
-- Name: idx_transformation_impacts_dataset_file; Type: INDEX; Schema: lineage; Owner: -
--

CREATE INDEX idx_transformation_impacts_dataset_file ON lineage.transformation_impacts USING btree (dataset_file_id);


--
-- Name: idx_transformation_impacts_dataset_version; Type: INDEX; Schema: lineage; Owner: -
--

CREATE INDEX idx_transformation_impacts_dataset_version ON lineage.transformation_impacts USING btree (dataset_id, partition_value, recorded_at);


--
-- Name: idx_transformation_impacts_operation; Type: INDEX; Schema: lineage; Owner: -
--

CREATE INDEX idx_transformation_impacts_operation ON lineage.transformation_impacts USING btree (operation, column_name);


--
-- Name: idx_transformation_impacts_pipeline_run; Type: INDEX; Schema: lineage; Owner: -
--

CREATE INDEX idx_transformation_impacts_pipeline_run ON lineage.transformation_impacts USING btree (pipeline_run_id);


--
-- Name: idx_transformation_impacts_silver_build; Type: INDEX; Schema: lineage; Owner: -
--

CREATE INDEX idx_transformation_impacts_silver_build ON lineage.transformation_impacts USING btree (silver_build_id, table_key, column_name);


--
-- Name: idx_execution_logs_pipeline_run_id; Type: INDEX; Schema: logs; Owner: -
--

CREATE INDEX idx_execution_logs_pipeline_run_id ON logs.execution_logs USING btree (pipeline_run_id, ts, execution_event_id);


--
-- Name: idx_execution_logs_silver_build; Type: INDEX; Schema: logs; Owner: -
--

CREATE INDEX idx_execution_logs_silver_build ON logs.execution_logs USING btree (silver_build_id, ts, execution_event_id);


--
-- Name: idx_marshal_events_file_order; Type: INDEX; Schema: logs; Owner: -
--

CREATE INDEX idx_marshal_events_file_order ON logs.marshal_events USING btree (file_id, event_ts, marshal_event_id);


--
-- Name: idx_marshal_events_file_reason; Type: INDEX; Schema: logs; Owner: -
--

CREATE INDEX idx_marshal_events_file_reason ON logs.marshal_events USING btree (file_id, reason);


--
-- Name: idx_marshal_events_order; Type: INDEX; Schema: logs; Owner: -
--

CREATE INDEX idx_marshal_events_order ON logs.marshal_events USING btree (event_ts, marshal_event_id);


--
-- Name: idx_pipeline_runs_source_capture; Type: INDEX; Schema: logs; Owner: -
--

CREATE INDEX idx_pipeline_runs_source_capture ON logs.pipeline_runs USING btree (source_capture_id);


--
-- Name: idx_pipeline_runs_workspace_started; Type: INDEX; Schema: logs; Owner: -
--

CREATE INDEX idx_pipeline_runs_workspace_started ON logs.pipeline_runs USING btree (workspace_name, started_at DESC);


--
-- Name: idx_silver_build_attempts_dataset_file; Type: INDEX; Schema: logs; Owner: -
--

CREATE INDEX idx_silver_build_attempts_dataset_file ON logs.silver_build_attempts USING btree (dataset_file_id, started_at DESC);


--
-- Name: idx_silver_build_attempts_dataset_version; Type: INDEX; Schema: logs; Owner: -
--

CREATE INDEX idx_silver_build_attempts_dataset_version ON logs.silver_build_attempts USING btree (dataset_id, partition_value, started_at DESC);


--
-- Name: idx_silver_build_attempts_successful_signature; Type: INDEX; Schema: logs; Owner: -
--

CREATE INDEX idx_silver_build_attempts_successful_signature ON logs.silver_build_attempts USING btree (build_signature, completed_at DESC) WHERE (status = 'succeeded'::text);


--
-- Name: idx_marshaled_files_dataset_hash; Type: INDEX; Schema: meta; Owner: -
--

CREATE INDEX idx_marshaled_files_dataset_hash ON meta.marshaled_files USING btree (dataset_id, source_hash);


--
-- Name: idx_marshaled_files_dataset_period_promoted; Type: INDEX; Schema: meta; Owner: -
--

CREATE INDEX idx_marshaled_files_dataset_period_promoted ON meta.marshaled_files USING btree (dataset_id, version_period, is_promoted);


--
-- Name: idx_silver_engine_releases_detected; Type: INDEX; Schema: meta; Owner: -
--

CREATE INDEX idx_silver_engine_releases_detected ON meta.silver_engine_releases USING btree (detected_at);


--
-- Name: idx_silver_materializations_fingerprint; Type: INDEX; Schema: meta; Owner: -
--

CREATE INDEX idx_silver_materializations_fingerprint ON meta.silver_materializations USING btree (logical_data_hash, schema_hash);


--
-- Name: idx_silver_materializations_materialized_at; Type: INDEX; Schema: meta; Owner: -
--

CREATE INDEX idx_silver_materializations_materialized_at ON meta.silver_materializations USING btree (materialized_at DESC);


--
-- Name: idx_source_capture_assets_dataset; Type: INDEX; Schema: meta; Owner: -
--

CREATE INDEX idx_source_capture_assets_dataset ON meta.source_capture_assets USING btree (dataset_id);


--
-- Name: idx_source_capture_assets_file; Type: INDEX; Schema: meta; Owner: -
--

CREATE INDEX idx_source_capture_assets_file ON meta.source_capture_assets USING btree (dataset_file_id);


--
-- Name: idx_source_schema_snapshots_dataset; Type: INDEX; Schema: meta; Owner: -
--

CREATE INDEX idx_source_schema_snapshots_dataset ON meta.source_schema_snapshots USING btree (dataset_id, parsed_at DESC);


--
-- Name: idx_source_schema_snapshots_hash; Type: INDEX; Schema: meta; Owner: -
--

CREATE INDEX idx_source_schema_snapshots_hash ON meta.source_schema_snapshots USING btree (schema_hash_algorithm, schema_hash);


--
-- Name: uq_marshaled_files_promoted_period; Type: INDEX; Schema: meta; Owner: -
--

CREATE UNIQUE INDEX uq_marshaled_files_promoted_period ON meta.marshaled_files USING btree (dataset_id, version_period) WHERE ((is_promoted = true) AND (version_period IS NOT NULL));


--
-- Name: uq_silver_engine_single_approved; Type: INDEX; Schema: meta; Owner: -
--

CREATE UNIQUE INDEX uq_silver_engine_single_approved ON meta.silver_engine_releases USING btree (status) WHERE (status = 'approved'::text);


--
-- Name: idx_asset_integrity_batches_checked_at; Type: INDEX; Schema: quality; Owner: -
--

CREATE INDEX idx_asset_integrity_batches_checked_at ON quality.asset_integrity_batches USING btree (checked_at, integrity_batch_id);


--
-- Name: idx_asset_integrity_results_failures; Type: INDEX; Schema: quality; Owner: -
--

CREATE INDEX idx_asset_integrity_results_failures ON quality.asset_integrity_results USING btree (integrity_batch_id, file_path) WHERE (status = 'failed'::text);


--
-- Name: idx_publication_gate_attempts_build; Type: INDEX; Schema: quality; Owner: -
--

CREATE INDEX idx_publication_gate_attempts_build ON quality.publication_gate_attempts USING btree (silver_build_id, gate_attempt_id);


--
-- Name: idx_publication_gate_attempts_candidate; Type: INDEX; Schema: quality; Owner: -
--

CREATE INDEX idx_publication_gate_attempts_candidate ON quality.publication_gate_attempts USING btree (candidate_id, gate_attempt_id);


--
-- Name: idx_publication_integrity_checks_publication; Type: INDEX; Schema: quality; Owner: -
--

CREATE INDEX idx_publication_integrity_checks_publication ON quality.publication_integrity_checks USING btree (publication_id, verification_trigger, integrity_batch_id);


--
-- Name: idx_quality_check_runs_dataset; Type: INDEX; Schema: quality; Owner: -
--

CREATE INDEX idx_quality_check_runs_dataset ON quality.quality_check_runs USING btree (dataset_id, layer, executed_at DESC);


--
-- Name: idx_quality_check_runs_file; Type: INDEX; Schema: quality; Owner: -
--

CREATE INDEX idx_quality_check_runs_file ON quality.quality_check_runs USING btree (dataset_file_id, layer, status);


--
-- Name: idx_quality_check_runs_pipeline_run_id; Type: INDEX; Schema: quality; Owner: -
--

CREATE INDEX idx_quality_check_runs_pipeline_run_id ON quality.quality_check_runs USING btree (pipeline_run_id);


--
-- Name: idx_quality_check_runs_run; Type: INDEX; Schema: quality; Owner: -
--

CREATE INDEX idx_quality_check_runs_run ON quality.quality_check_runs USING btree (run_id, step_id);


--
-- Name: idx_quality_check_runs_silver_build; Type: INDEX; Schema: quality; Owner: -
--

CREATE INDEX idx_quality_check_runs_silver_build ON quality.quality_check_runs USING btree (silver_build_id, check_id);


--
-- Name: idx_silver_verifications_last_verified; Type: INDEX; Schema: quality; Owner: -
--

CREATE INDEX idx_silver_verifications_last_verified ON quality.silver_publication_verifications USING btree (last_verified_at);


--
-- Name: idx_silver_verifications_latest_build; Type: INDEX; Schema: quality; Owner: -
--

CREATE INDEX idx_silver_verifications_latest_build ON quality.silver_publication_verifications USING btree (latest_silver_build_id);


--
-- Name: dataset_category_memberships dataset_category_memberships_category_slug_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_category_memberships
    ADD CONSTRAINT dataset_category_memberships_category_slug_fkey FOREIGN KEY (category_slug) REFERENCES catalog.dataset_categories(category_slug);


--
-- Name: dataset_publication_assets dataset_publication_assets_publication_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publication_assets
    ADD CONSTRAINT dataset_publication_assets_publication_fkey FOREIGN KEY (publication_id) REFERENCES catalog.dataset_publications(publication_id) ON DELETE CASCADE;


--
-- Name: dataset_publications dataset_publications_engine_release_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publications
    ADD CONSTRAINT dataset_publications_engine_release_fkey FOREIGN KEY (engine_release_id) REFERENCES meta.silver_engine_releases(engine_release_id) ON DELETE RESTRICT;


--
-- Name: dataset_publications dataset_publications_silver_materialization_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publications
    ADD CONSTRAINT dataset_publications_silver_materialization_fkey FOREIGN KEY (silver_build_id) REFERENCES meta.silver_materializations(silver_build_id) ON DELETE RESTRICT;


--
-- Name: dataset_publications dataset_publications_supersedes_publication_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publications
    ADD CONSTRAINT dataset_publications_supersedes_publication_id_fkey FOREIGN KEY (supersedes_publication_id) REFERENCES catalog.dataset_publications(publication_id);


--
-- Name: dataset_tag_memberships dataset_tag_memberships_tag_slug_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_tag_memberships
    ADD CONSTRAINT dataset_tag_memberships_tag_slug_fkey FOREIGN KEY (tag_slug) REFERENCES catalog.dataset_tags(tag_slug);


--
-- Name: dataset_publication_candidates publication_candidates_baseline_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publication_candidates
    ADD CONSTRAINT publication_candidates_baseline_fkey FOREIGN KEY (baseline_publication_id) REFERENCES catalog.dataset_publications(publication_id) ON DELETE RESTRICT;


--
-- Name: dataset_publication_candidates publication_candidates_publication_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publication_candidates
    ADD CONSTRAINT publication_candidates_publication_fkey FOREIGN KEY (publication_id) REFERENCES catalog.dataset_publications(publication_id) ON DELETE RESTRICT;


--
-- Name: dataset_publication_candidates publication_candidates_silver_materialization_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publication_candidates
    ADD CONSTRAINT publication_candidates_silver_materialization_fkey FOREIGN KEY (silver_build_id) REFERENCES meta.silver_materializations(silver_build_id) ON DELETE RESTRICT;


--
-- Name: dataset_publication_projection_states publication_projection_expected_publication_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publication_projection_states
    ADD CONSTRAINT publication_projection_expected_publication_fkey FOREIGN KEY (expected_publication_id) REFERENCES catalog.dataset_publications(publication_id) ON DELETE RESTRICT;


--
-- Name: dataset_publication_projection_states publication_projection_projected_publication_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.dataset_publication_projection_states
    ADD CONSTRAINT publication_projection_projected_publication_fkey FOREIGN KEY (projected_publication_id) REFERENCES catalog.dataset_publications(publication_id) ON DELETE RESTRICT;


--
-- Name: transformation_impacts transformation_impacts_dataset_file_fkey; Type: FK CONSTRAINT; Schema: lineage; Owner: -
--

ALTER TABLE ONLY lineage.transformation_impacts
    ADD CONSTRAINT transformation_impacts_dataset_file_fkey FOREIGN KEY (dataset_file_id) REFERENCES meta.marshaled_files(dataset_file_id) ON DELETE CASCADE;


--
-- Name: transformation_impacts transformation_impacts_pipeline_run_fkey; Type: FK CONSTRAINT; Schema: lineage; Owner: -
--

ALTER TABLE ONLY lineage.transformation_impacts
    ADD CONSTRAINT transformation_impacts_pipeline_run_fkey FOREIGN KEY (pipeline_run_id) REFERENCES logs.pipeline_runs(pipeline_run_id) ON DELETE CASCADE;


--
-- Name: transformation_impacts transformation_impacts_silver_build_attempt_fkey; Type: FK CONSTRAINT; Schema: lineage; Owner: -
--

ALTER TABLE ONLY lineage.transformation_impacts
    ADD CONSTRAINT transformation_impacts_silver_build_attempt_fkey FOREIGN KEY (silver_build_id) REFERENCES logs.silver_build_attempts(silver_build_id) ON DELETE RESTRICT;


--
-- Name: execution_logs execution_logs_silver_build_attempt_fkey; Type: FK CONSTRAINT; Schema: logs; Owner: -
--

ALTER TABLE ONLY logs.execution_logs
    ADD CONSTRAINT execution_logs_silver_build_attempt_fkey FOREIGN KEY (silver_build_id) REFERENCES logs.silver_build_attempts(silver_build_id) ON DELETE RESTRICT;


--
-- Name: pipeline_runs pipeline_runs_source_capture_id_fkey; Type: FK CONSTRAINT; Schema: logs; Owner: -
--

ALTER TABLE ONLY logs.pipeline_runs
    ADD CONSTRAINT pipeline_runs_source_capture_id_fkey FOREIGN KEY (source_capture_id) REFERENCES meta.source_captures(source_capture_id);


--
-- Name: silver_build_attempts silver_build_attempts_dataset_file_fkey; Type: FK CONSTRAINT; Schema: logs; Owner: -
--

ALTER TABLE ONLY logs.silver_build_attempts
    ADD CONSTRAINT silver_build_attempts_dataset_file_fkey FOREIGN KEY (dataset_file_id) REFERENCES meta.marshaled_files(dataset_file_id) ON DELETE CASCADE;


--
-- Name: silver_build_attempts silver_build_attempts_engine_release_fkey; Type: FK CONSTRAINT; Schema: logs; Owner: -
--

ALTER TABLE ONLY logs.silver_build_attempts
    ADD CONSTRAINT silver_build_attempts_engine_release_fkey FOREIGN KEY (engine_release_id) REFERENCES meta.silver_engine_releases(engine_release_id) ON DELETE RESTRICT;


--
-- Name: silver_build_attempts silver_build_attempts_pipeline_run_fkey; Type: FK CONSTRAINT; Schema: logs; Owner: -
--

ALTER TABLE ONLY logs.silver_build_attempts
    ADD CONSTRAINT silver_build_attempts_pipeline_run_fkey FOREIGN KEY (pipeline_run_id) REFERENCES logs.pipeline_runs(pipeline_run_id) ON DELETE RESTRICT;


--
-- Name: silver_materializations silver_materializations_attempt_fkey; Type: FK CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.silver_materializations
    ADD CONSTRAINT silver_materializations_attempt_fkey FOREIGN KEY (silver_build_id) REFERENCES logs.silver_build_attempts(silver_build_id) ON DELETE RESTRICT;


--
-- Name: source_capture_assets source_capture_assets_capture_fkey; Type: FK CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_capture_assets
    ADD CONSTRAINT source_capture_assets_capture_fkey FOREIGN KEY (source_capture_id) REFERENCES meta.source_captures(source_capture_id) ON DELETE CASCADE;


--
-- Name: source_capture_assets source_capture_assets_file_fkey; Type: FK CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_capture_assets
    ADD CONSTRAINT source_capture_assets_file_fkey FOREIGN KEY (dataset_file_id) REFERENCES meta.marshaled_files(dataset_file_id);


--
-- Name: source_schema_bindings source_schema_bindings_schema_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_schema_bindings
    ADD CONSTRAINT source_schema_bindings_schema_snapshot_id_fkey FOREIGN KEY (schema_snapshot_id) REFERENCES meta.source_schema_snapshots(schema_snapshot_id);


--
-- Name: source_schema_fields source_schema_fields_schema_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: meta; Owner: -
--

ALTER TABLE ONLY meta.source_schema_fields
    ADD CONSTRAINT source_schema_fields_schema_snapshot_id_fkey FOREIGN KEY (schema_snapshot_id) REFERENCES meta.source_schema_snapshots(schema_snapshot_id) ON DELETE CASCADE;


--
-- Name: asset_integrity_results asset_integrity_results_batch_fkey; Type: FK CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.asset_integrity_results
    ADD CONSTRAINT asset_integrity_results_batch_fkey FOREIGN KEY (integrity_batch_id) REFERENCES quality.asset_integrity_batches(integrity_batch_id) ON DELETE RESTRICT;


--
-- Name: publication_gate_attempts publication_gate_attempts_candidate_fkey; Type: FK CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.publication_gate_attempts
    ADD CONSTRAINT publication_gate_attempts_candidate_fkey FOREIGN KEY (candidate_id, silver_build_id) REFERENCES catalog.dataset_publication_candidates(candidate_id, silver_build_id) ON DELETE RESTRICT;


--
-- Name: publication_gate_attempts publication_gate_attempts_integrity_batch_fkey; Type: FK CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.publication_gate_attempts
    ADD CONSTRAINT publication_gate_attempts_integrity_batch_fkey FOREIGN KEY (integrity_batch_id) REFERENCES quality.asset_integrity_batches(integrity_batch_id) ON DELETE RESTRICT;


--
-- Name: publication_gate_attempts publication_gate_attempts_pipeline_run_fkey; Type: FK CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.publication_gate_attempts
    ADD CONSTRAINT publication_gate_attempts_pipeline_run_fkey FOREIGN KEY (pipeline_run_id) REFERENCES logs.pipeline_runs(pipeline_run_id) ON DELETE RESTRICT;


--
-- Name: publication_integrity_checks publication_integrity_checks_batch_fkey; Type: FK CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.publication_integrity_checks
    ADD CONSTRAINT publication_integrity_checks_batch_fkey FOREIGN KEY (integrity_batch_id) REFERENCES quality.asset_integrity_batches(integrity_batch_id) ON DELETE RESTRICT;


--
-- Name: publication_integrity_checks publication_integrity_checks_publication_fkey; Type: FK CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.publication_integrity_checks
    ADD CONSTRAINT publication_integrity_checks_publication_fkey FOREIGN KEY (publication_id) REFERENCES catalog.dataset_publications(publication_id) ON DELETE RESTRICT;


--
-- Name: quality_check_runs quality_check_runs_check_id_fkey; Type: FK CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.quality_check_runs
    ADD CONSTRAINT quality_check_runs_check_id_fkey FOREIGN KEY (check_id) REFERENCES quality.quality_check_definitions(check_id);


--
-- Name: quality_check_runs quality_check_runs_silver_build_attempt_fkey; Type: FK CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.quality_check_runs
    ADD CONSTRAINT quality_check_runs_silver_build_attempt_fkey FOREIGN KEY (silver_build_id) REFERENCES logs.silver_build_attempts(silver_build_id) ON DELETE RESTRICT;


--
-- Name: silver_publication_verifications silver_verifications_latest_materialization_fkey; Type: FK CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.silver_publication_verifications
    ADD CONSTRAINT silver_verifications_latest_materialization_fkey FOREIGN KEY (latest_silver_build_id) REFERENCES meta.silver_materializations(silver_build_id) ON DELETE RESTRICT;


--
-- Name: silver_publication_verifications silver_verifications_publication_fkey; Type: FK CONSTRAINT; Schema: quality; Owner: -
--

ALTER TABLE ONLY quality.silver_publication_verifications
    ADD CONSTRAINT silver_verifications_publication_fkey FOREIGN KEY (publication_id) REFERENCES catalog.dataset_publications(publication_id) ON DELETE RESTRICT;


--
-- Name: SCHEMA catalog; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA catalog TO metrka_etl;
GRANT USAGE ON SCHEMA catalog TO metrka_web;
GRANT USAGE ON SCHEMA catalog TO metrka_operator;


--
-- Name: SCHEMA lineage; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA lineage TO metrka_etl;
GRANT USAGE ON SCHEMA lineage TO metrka_web;
GRANT USAGE ON SCHEMA lineage TO metrka_operator;


--
-- Name: SCHEMA logs; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA logs TO metrka_etl;
GRANT USAGE ON SCHEMA logs TO metrka_web;
GRANT USAGE ON SCHEMA logs TO metrka_operator;


--
-- Name: SCHEMA meta; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA meta TO metrka_etl;
GRANT USAGE ON SCHEMA meta TO metrka_web;
GRANT USAGE ON SCHEMA meta TO metrka_operator;


--
-- Name: SCHEMA quality; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA quality TO metrka_etl;
GRANT USAGE ON SCHEMA quality TO metrka_web;
GRANT USAGE ON SCHEMA quality TO metrka_operator;


--
-- Name: TABLE dataset_categories; Type: ACL; Schema: catalog; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE catalog.dataset_categories TO metrka_etl;
GRANT SELECT ON TABLE catalog.dataset_categories TO metrka_web;
GRANT SELECT ON TABLE catalog.dataset_categories TO metrka_operator;


--
-- Name: TABLE dataset_category_memberships; Type: ACL; Schema: catalog; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE catalog.dataset_category_memberships TO metrka_etl;
GRANT SELECT ON TABLE catalog.dataset_category_memberships TO metrka_web;
GRANT SELECT ON TABLE catalog.dataset_category_memberships TO metrka_operator;


--
-- Name: TABLE dataset_publication_assets; Type: ACL; Schema: catalog; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE catalog.dataset_publication_assets TO metrka_etl;
GRANT SELECT ON TABLE catalog.dataset_publication_assets TO metrka_web;
GRANT SELECT,INSERT ON TABLE catalog.dataset_publication_assets TO metrka_operator;


--
-- Name: TABLE dataset_publication_candidates; Type: ACL; Schema: catalog; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE catalog.dataset_publication_candidates TO metrka_etl;
GRANT SELECT ON TABLE catalog.dataset_publication_candidates TO metrka_web;
GRANT SELECT,UPDATE ON TABLE catalog.dataset_publication_candidates TO metrka_operator;


--
-- Name: TABLE dataset_publication_projection_states; Type: ACL; Schema: catalog; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE catalog.dataset_publication_projection_states TO metrka_etl;
GRANT SELECT ON TABLE catalog.dataset_publication_projection_states TO metrka_web;
GRANT SELECT,INSERT,UPDATE ON TABLE catalog.dataset_publication_projection_states TO metrka_operator;


--
-- Name: TABLE dataset_publications; Type: ACL; Schema: catalog; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE catalog.dataset_publications TO metrka_etl;
GRANT SELECT ON TABLE catalog.dataset_publications TO metrka_web;
GRANT SELECT,INSERT,UPDATE ON TABLE catalog.dataset_publications TO metrka_operator;


--
-- Name: TABLE dataset_tag_memberships; Type: ACL; Schema: catalog; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE catalog.dataset_tag_memberships TO metrka_etl;
GRANT SELECT ON TABLE catalog.dataset_tag_memberships TO metrka_web;
GRANT SELECT ON TABLE catalog.dataset_tag_memberships TO metrka_operator;


--
-- Name: TABLE dataset_tags; Type: ACL; Schema: catalog; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE catalog.dataset_tags TO metrka_etl;
GRANT SELECT ON TABLE catalog.dataset_tags TO metrka_web;
GRANT SELECT ON TABLE catalog.dataset_tags TO metrka_operator;


--
-- Name: TABLE transformation_impacts; Type: ACL; Schema: lineage; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE lineage.transformation_impacts TO metrka_etl;
GRANT SELECT ON TABLE lineage.transformation_impacts TO metrka_web;
GRANT SELECT ON TABLE lineage.transformation_impacts TO metrka_operator;


--
-- Name: TABLE execution_logs; Type: ACL; Schema: logs; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE logs.execution_logs TO metrka_etl;
GRANT SELECT ON TABLE logs.execution_logs TO metrka_web;
GRANT SELECT ON TABLE logs.execution_logs TO metrka_operator;


--
-- Name: SEQUENCE execution_logs_execution_event_id_seq; Type: ACL; Schema: logs; Owner: -
--

GRANT ALL ON SEQUENCE logs.execution_logs_execution_event_id_seq TO metrka_etl;


--
-- Name: TABLE marshal_events; Type: ACL; Schema: logs; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE logs.marshal_events TO metrka_etl;
GRANT SELECT ON TABLE logs.marshal_events TO metrka_web;
GRANT SELECT ON TABLE logs.marshal_events TO metrka_operator;


--
-- Name: SEQUENCE marshal_events_marshal_event_id_seq; Type: ACL; Schema: logs; Owner: -
--

GRANT ALL ON SEQUENCE logs.marshal_events_marshal_event_id_seq TO metrka_etl;


--
-- Name: TABLE pipeline_runs; Type: ACL; Schema: logs; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE logs.pipeline_runs TO metrka_etl;
GRANT SELECT ON TABLE logs.pipeline_runs TO metrka_web;
GRANT SELECT ON TABLE logs.pipeline_runs TO metrka_operator;


--
-- Name: TABLE silver_build_attempts; Type: ACL; Schema: logs; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE logs.silver_build_attempts TO metrka_etl;
GRANT SELECT ON TABLE logs.silver_build_attempts TO metrka_web;
GRANT SELECT ON TABLE logs.silver_build_attempts TO metrka_operator;


--
-- Name: TABLE contract_snapshots; Type: ACL; Schema: meta; Owner: -
--

GRANT SELECT,INSERT ON TABLE meta.contract_snapshots TO metrka_etl;
GRANT SELECT ON TABLE meta.contract_snapshots TO metrka_web;
GRANT SELECT ON TABLE meta.contract_snapshots TO metrka_operator;


--
-- Name: TABLE marshaled_files; Type: ACL; Schema: meta; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE meta.marshaled_files TO metrka_etl;
GRANT SELECT ON TABLE meta.marshaled_files TO metrka_web;
GRANT SELECT ON TABLE meta.marshaled_files TO metrka_operator;


--
-- Name: TABLE silver_engine_releases; Type: ACL; Schema: meta; Owner: -
--

GRANT SELECT,INSERT ON TABLE meta.silver_engine_releases TO metrka_etl;
GRANT SELECT ON TABLE meta.silver_engine_releases TO metrka_web;
GRANT SELECT,UPDATE ON TABLE meta.silver_engine_releases TO metrka_operator;


--
-- Name: TABLE silver_materializations; Type: ACL; Schema: meta; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE meta.silver_materializations TO metrka_etl;
GRANT SELECT ON TABLE meta.silver_materializations TO metrka_web;
GRANT SELECT ON TABLE meta.silver_materializations TO metrka_operator;


--
-- Name: TABLE source_capture_assets; Type: ACL; Schema: meta; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE meta.source_capture_assets TO metrka_etl;
GRANT SELECT ON TABLE meta.source_capture_assets TO metrka_web;
GRANT SELECT ON TABLE meta.source_capture_assets TO metrka_operator;


--
-- Name: TABLE source_captures; Type: ACL; Schema: meta; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE meta.source_captures TO metrka_etl;
GRANT SELECT ON TABLE meta.source_captures TO metrka_web;
GRANT SELECT ON TABLE meta.source_captures TO metrka_operator;


--
-- Name: TABLE source_schema_bindings; Type: ACL; Schema: meta; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE meta.source_schema_bindings TO metrka_etl;
GRANT SELECT ON TABLE meta.source_schema_bindings TO metrka_web;
GRANT SELECT ON TABLE meta.source_schema_bindings TO metrka_operator;


--
-- Name: TABLE source_schema_fields; Type: ACL; Schema: meta; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE meta.source_schema_fields TO metrka_etl;
GRANT SELECT ON TABLE meta.source_schema_fields TO metrka_web;
GRANT SELECT ON TABLE meta.source_schema_fields TO metrka_operator;


--
-- Name: TABLE source_schema_snapshots; Type: ACL; Schema: meta; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE meta.source_schema_snapshots TO metrka_etl;
GRANT SELECT ON TABLE meta.source_schema_snapshots TO metrka_web;
GRANT SELECT ON TABLE meta.source_schema_snapshots TO metrka_operator;


--
-- Name: TABLE asset_integrity_batches; Type: ACL; Schema: quality; Owner: -
--

GRANT SELECT,INSERT ON TABLE quality.asset_integrity_batches TO metrka_etl;
GRANT SELECT ON TABLE quality.asset_integrity_batches TO metrka_web;
GRANT SELECT,INSERT ON TABLE quality.asset_integrity_batches TO metrka_operator;


--
-- Name: SEQUENCE asset_integrity_batches_integrity_batch_id_seq; Type: ACL; Schema: quality; Owner: -
--

GRANT ALL ON SEQUENCE quality.asset_integrity_batches_integrity_batch_id_seq TO metrka_etl;
GRANT SELECT,USAGE ON SEQUENCE quality.asset_integrity_batches_integrity_batch_id_seq TO metrka_operator;


--
-- Name: TABLE asset_integrity_results; Type: ACL; Schema: quality; Owner: -
--

GRANT SELECT,INSERT ON TABLE quality.asset_integrity_results TO metrka_etl;
GRANT SELECT ON TABLE quality.asset_integrity_results TO metrka_web;
GRANT SELECT,INSERT ON TABLE quality.asset_integrity_results TO metrka_operator;


--
-- Name: TABLE publication_gate_attempts; Type: ACL; Schema: quality; Owner: -
--

GRANT SELECT,INSERT ON TABLE quality.publication_gate_attempts TO metrka_etl;
GRANT SELECT ON TABLE quality.publication_gate_attempts TO metrka_web;
GRANT SELECT,INSERT ON TABLE quality.publication_gate_attempts TO metrka_operator;


--
-- Name: SEQUENCE publication_gate_attempts_gate_attempt_id_seq; Type: ACL; Schema: quality; Owner: -
--

GRANT ALL ON SEQUENCE quality.publication_gate_attempts_gate_attempt_id_seq TO metrka_etl;
GRANT SELECT,USAGE ON SEQUENCE quality.publication_gate_attempts_gate_attempt_id_seq TO metrka_operator;


--
-- Name: TABLE publication_integrity_checks; Type: ACL; Schema: quality; Owner: -
--

GRANT SELECT,INSERT ON TABLE quality.publication_integrity_checks TO metrka_etl;
GRANT SELECT ON TABLE quality.publication_integrity_checks TO metrka_web;
GRANT SELECT,INSERT ON TABLE quality.publication_integrity_checks TO metrka_operator;


--
-- Name: TABLE quality_check_definitions; Type: ACL; Schema: quality; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE quality.quality_check_definitions TO metrka_etl;
GRANT SELECT ON TABLE quality.quality_check_definitions TO metrka_web;
GRANT SELECT ON TABLE quality.quality_check_definitions TO metrka_operator;


--
-- Name: TABLE quality_check_runs; Type: ACL; Schema: quality; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE quality.quality_check_runs TO metrka_etl;
GRANT SELECT ON TABLE quality.quality_check_runs TO metrka_web;
GRANT SELECT ON TABLE quality.quality_check_runs TO metrka_operator;


--
-- Name: TABLE silver_publication_verifications; Type: ACL; Schema: quality; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE quality.silver_publication_verifications TO metrka_etl;
GRANT SELECT ON TABLE quality.silver_publication_verifications TO metrka_web;
GRANT SELECT ON TABLE quality.silver_publication_verifications TO metrka_operator;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: catalog; Owner: -
--

ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA catalog GRANT ALL ON SEQUENCES TO metrka_etl;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: catalog; Owner: -
--

ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA catalog GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES TO metrka_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA catalog GRANT SELECT ON TABLES TO metrka_web;
ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA catalog GRANT SELECT ON TABLES TO metrka_operator;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: lineage; Owner: -
--

ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA lineage GRANT ALL ON SEQUENCES TO metrka_etl;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: lineage; Owner: -
--

ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA lineage GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES TO metrka_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA lineage GRANT SELECT ON TABLES TO metrka_web;
ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA lineage GRANT SELECT ON TABLES TO metrka_operator;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: logs; Owner: -
--

ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA logs GRANT ALL ON SEQUENCES TO metrka_etl;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: logs; Owner: -
--

ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA logs GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES TO metrka_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA logs GRANT SELECT ON TABLES TO metrka_web;
ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA logs GRANT SELECT ON TABLES TO metrka_operator;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: meta; Owner: -
--

ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA meta GRANT ALL ON SEQUENCES TO metrka_etl;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: meta; Owner: -
--

ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA meta GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES TO metrka_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA meta GRANT SELECT ON TABLES TO metrka_web;
ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA meta GRANT SELECT ON TABLES TO metrka_operator;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: quality; Owner: -
--

ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA quality GRANT ALL ON SEQUENCES TO metrka_etl;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: quality; Owner: -
--

ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA quality GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES TO metrka_etl;
ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA quality GRANT SELECT ON TABLES TO metrka_web;
ALTER DEFAULT PRIVILEGES FOR ROLE metrka_owner IN SCHEMA quality GRANT SELECT ON TABLES TO metrka_operator;


--
-- PostgreSQL database dump complete
--


