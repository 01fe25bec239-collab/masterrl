-- P19 saved a verified blob for each file before the registry recorded its old staging key.
UPDATE model_files
   SET object_key = 'orgs/' || split_part(object_key, '/', 2) || '/blobs/sha256/'
       || substr(encode(sha256, 'hex'), 1, 2) || '/'
       || substr(encode(sha256, 'hex'), 3, 2) || '/' || encode(sha256, 'hex')
 WHERE object_key LIKE 'orgs/%' AND object_key !~ '^orgs/[^/]+/blobs/sha256/';

CREATE INDEX model_files_object_key ON model_files (object_key);
