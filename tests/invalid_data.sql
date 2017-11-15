--
-- PostgreSQL database dump
--

CREATE TABLE _nice_binary (
    hash character(40) NOT NULL,
    size bigint NOT NULL,
    mime_type character varying(255) NOT NULL,
    data oid,
    sha2 character(64)
);

SELECT pg_catalog.lo_create('198485881');
SELECT pg_catalog.lo_create('198485882');
SELECT pg_catalog.lo_create('198485883');
SELECT pg_catalog.lo_create('198485884');
SELECT pg_catalog.lo_create('198485885');

INSERT INTO _nice_binary (hash, size, mime_type, data, sha2)
VALUES
        -- hash too short
    ('392007d934dd54600110b683f5e52aeeedb1',  125, 'octet/stream', 198485884, null),

        -- incorrect hash
    ('0000000042b415eaf546865acf6287fc8ac1f0e5',   81, 'octet/stream', 198485885, null),

        -- invalid hash (not hex)
    ('d0706a2d42b415eaX546865acf6287fc8ac1f0e5',   81, 'octet/stream', 198485886, null),

        -- missing Large Object
    ('d66f1ca50b9fbfb44f3190beca607da08f18a842', 133, '', 198485887, null);

BEGIN;

SELECT pg_catalog.lo_open('198485885', 131072);
SELECT pg_catalog.lowrite(0, '\xe8c9d2f86d05deb380d479aa8a3886f3900f0a32904972ab18cef6a3412af6080bd78f71a492a6200fa8250501ef5c21cd11efe2bbce05ed55f5c858f28493da693dc613eb8d0f2fc426b24e6a5cb56c87');
SELECT pg_catalog.lo_close(0);

COMMIT;
