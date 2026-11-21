-- 0011_deferrable_media_asset_path_unique.sql: Make the media_assets
-- (library_root_id, rel_path_bytes) unique constraint DEFERRABLE INITIALLY IMMEDIATE
-- so atomic path exchanges and rename chains can defer uniqueness within one
-- reconciliation transaction while keeping ordinary writes immediately checked.

ALTER TABLE media_assets
    DROP CONSTRAINT media_assets_library_root_id_rel_path_bytes_key,
    ADD CONSTRAINT media_assets_library_root_id_rel_path_bytes_key
        UNIQUE (library_root_id, rel_path_bytes)
        DEFERRABLE INITIALLY IMMEDIATE;
