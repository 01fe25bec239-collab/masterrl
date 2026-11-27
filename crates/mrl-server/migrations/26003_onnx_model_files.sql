ALTER TABLE model_files
  DROP CONSTRAINT model_files_format_check,
  ADD CONSTRAINT model_files_format_check
  CHECK (format IN ('safetensors', 'gguf', 'onnx', 'json', 'txt', 'other'));
