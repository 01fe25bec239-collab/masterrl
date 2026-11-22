"""Training checks and per-dataset preprocessing for the pinned TRL trainer."""

# pyright: reportMissingImports=false

from copy import copy
from pathlib import Path
from typing import Any

import torch
from datasets import concatenate_datasets
from trl import SFTTrainer
from trl.chat_template_utils import get_training_chat_template, has_generation_markers

MPS_RNG_FILE = "rng_state_mps.pth"


class SftTrainer(SFTTrainer):
    """Tokenize each input form with the right loss mask before combining it."""

    def __init__(
        self, *, datasets: dict[str, tuple[Any, ...] | None], mask_assistant: bool, **kwargs: Any
    ) -> None:
        self._dataset_groups = datasets
        self._mask_assistant = mask_assistant
        super().__init__(**kwargs)
        self._dataset_groups.clear()

    def _prepare_dataset(  # noqa: PLR0913, PLR0917 (TRL signature)
        self,
        dataset: Any,
        processing_class: Any,
        args: Any,
        packing: bool,
        formatting_func: Any,
        dataset_name: str,
    ) -> Any:
        groups = self._dataset_groups.get(dataset_name) or (dataset,)
        prepare = super()._prepare_dataset
        split_args = copy(args)
        split_args.assistant_only_loss = self._mask_assistant and "messages" in dataset.column_names
        template = self.chat_template
        if split_args.assistant_only_loss:
            if template is None and not has_generation_markers(processing_class.chat_template):
                self.chat_template = get_training_chat_template(processing_class)
        else:
            self.chat_template = None
        try:
            if len(groups) == 1:
                return prepare(
                    dataset, processing_class, split_args, packing, formatting_func, dataset_name
                )

            # TRL can pack across forms once every row has token IDs and labels.
            if packing:
                split_args.max_length = None
            columns = ["input_ids", "labels"]
            tokenized = [
                prepare(
                    part, processing_class, split_args, False, formatting_func, dataset_name
                ).select_columns(columns)
                for part in groups
            ]
            joined = concatenate_datasets(tokenized)
            if packing:
                return prepare(joined, processing_class, args, True, None, dataset_name)
            return joined
        finally:
            self.chat_template = template

    def training_step(self, *args: Any, **kwargs: Any) -> Any:
        loss = super().training_step(*args, **kwargs)
        if not torch.isfinite(loss).item():
            raise FloatingPointError("Training loss is not finite.")
        return loss

    def _save_rng_state(self, output_dir: str) -> None:
        super()._save_rng_state(output_dir)
        if self.args.device.type == "mps":
            torch.save(torch.mps.get_rng_state(), Path(output_dir) / MPS_RNG_FILE)

    def _load_rng_state(self, checkpoint: str | None) -> None:
        super()._load_rng_state(checkpoint)
        if checkpoint is not None and self.args.device.type == "mps":
            path = Path(checkpoint) / MPS_RNG_FILE
            if path.is_file():
                torch.mps.set_rng_state(torch.load(path, map_location="cpu", weights_only=True))
