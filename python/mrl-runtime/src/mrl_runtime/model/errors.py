"""The error type the P15 libraries raise: a catalog code plus a plain-language message."""


class MrlError(Exception):
    """An error with a guidance-catalog code (`MRL-<FAMILY>-<NNN>`) and plain-language text.

    `message` says what happened in words a user understands; `detail` names the file,
    field or value involved.
    """

    def __init__(self, code: str, message: str, detail: str = "") -> None:
        super().__init__(f"{code}: {message}" + (f" ({detail})" if detail else ""))
        self.code = code
        self.message = message
        self.detail = detail


# Codes this part raises (titles live in guidance/fragments/errors/p15.yaml or the catalog).
PICKLE_REFUSED = "MRL-MODEL-001"
REMOTE_CODE = "MRL-MODEL-002"
MODEL_UNREADABLE = "MRL-MODEL-150"
INFERENCE_ONLY = "MRL-MODEL-151"
TOKENIZER_UNUSABLE = "MRL-MODEL-152"
NO_CHAT_TEMPLATE = "MRL-CAP-016"
DATASET_INVALID = "MRL-DATA-001"
DATASET_UNREADABLE = "MRL-DATA-150"
COLUMN_TYPE_REFUSED = "MRL-DATA-151"
BAD_VALUE = "MRL-VAL-003"
FILE_IO = "MRL-STORE-005"
