"""Errors the runtime raises. Each carries a guidance code; the text lives in guidance/."""

# Codes this package owns (guidance/fragments/errors/p13.yaml).
MANIFEST_INVALID = "MRL-VAL-130"
TASK_INPUT_INVALID = "MRL-VAL-131"
TASK_NOT_INSTALLED = "MRL-CAP-130"
PROTOCOL_MISMATCH = "MRL-ENV-130"
ADAPTER_API_MISMATCH = "MRL-ENV-131"
PROTOCOL_VIOLATION = "MRL-INTERNAL-130"

# Shared codes reused with the same meaning.
FIELD_MISSING = "MRL-VAL-002"
FIELD_INVALID = "MRL-VAL-003"
METHOD_UNSUPPORTED = "MRL-CAP-001"
LOCAL_FILE_IO = "MRL-STORE-005"


class MrlError(Exception):
    """An error with a guidance code, a one-line message and an optional next step."""

    def __init__(self, code: str, message: str, *, hint: str = "", detail: str = "") -> None:
        super().__init__(f"{code}: {message}" + (f" {detail}" if detail else ""))
        self.code = code
        self.message = message
        self.hint = hint
        self.detail = detail
