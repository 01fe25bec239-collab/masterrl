ALTER TABLE license_acknowledgements
  DROP CONSTRAINT license_acknowledgements_pkey,
  ADD PRIMARY KEY (project_id, model_id, license_text_sha256);
