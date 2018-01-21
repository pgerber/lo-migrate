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
    ('ca837095b8e962525b988bd98afff27d104d7107', 10 * 1024 * 1024, '', 198485881, null),
    ('8bacf7b5aeb31e6d278598d4be2b263c8d53e862',  125, 'octet/stream', 198485882, null),

        -- already migrated
    ('0000000000000000000000000000000000000000',   44, 'unused',               0,
                                            '0000000000000000000000000000000000000000000000000000000000000000'),
    ('da39a3ee5e6b4b0d3255bfef95601890afd80709',    0, 'octet/stream', 198485883, null),
    ('4694849b7ceeae0e5254689ff1f352735581d6ec',   12, 'text/plain',   198485884, null),
    ('d0706a2d42b415eaf546865acf6287fc8ac1f0e5',   81, 'octet/stream', 198485885, null);
BEGIN;

SELECT pg_catalog.lo_open('198485881', 131072);
SELECT pg_catalog.lowrite(0, pg_catalog.decode(pg_catalog.repeat('01020304050607080910', 1024 * 1024), 'hex'));
SELECT pg_catalog.lo_close(0);

SELECT pg_catalog.lo_open('198485882', 131072);
SELECT pg_catalog.lowrite(0, '\xc888f353d096d359f050be81121ef57c30f94077d81af586565c0bcab0e5c2ef356654ae4f8c4efefffd5edea02a7f40044cf6b347913961a050d9239546e5eb18b52a01f381e058efa218a675a877223b34c1200b373eba02bf50b868fb4ef4db971040a219ba4420b826334f9ceb987592a1ee1338c50d16b3a590e3');
SELECT pg_catalog.lo_close(0);

SELECT pg_catalog.lo_open('198485883', 131072);
SELECT pg_catalog.lo_close(0);

SELECT pg_catalog.lo_open('198485884', 131072);
SELECT pg_catalog.lowrite(0, '\x6ca9df9f2e98068d369e8148');
SELECT pg_catalog.lo_close(0);

SELECT pg_catalog.lo_open('198485885', 131072);
SELECT pg_catalog.lowrite(0, '\xe8c9d2f86d05deb380d479aa8a3886f3900f0a32904972ab18cef6a3412af6080bd78f71a492a6200fa8250501ef5c21cd11efe2bbce05ed55f5c858f28493da693dc613eb8d0f2fc426b24e6a5cb56c87');
SELECT pg_catalog.lo_close(0);

COMMIT;
