# Writing rules

These rules apply to all text users see: UI, errors, docs and the guidance catalog.
The build (`guidance/tools/build.py`) checks the rules marked **checked**.

| Rule | Good | Bad |
|------|------|-----|
| Sentences have 20 words or fewer. **Checked.** | "The GPU ran out of memory." | One long sentence that explains the cause, the settings and three fixes at once. |
| One idea per sentence. | "The disk is full. Free up about 20 GB." | "The disk is full and you should free up space and then resume." |
| Talk to the user as "you". | "You can resume from the last checkpoint." | "The user can resume from the last checkpoint." |
| Use active voice. | "The app saved your checkpoint." | "Your checkpoint was saved by the app." |
| Buttons are verbs. | "Start training" | "Submit" |
| Numbers have units and ranges. | "about 20–30 minutes" | "soon" or "25" |
| Explain every acronym. Each one must be in the glossary. **Checked.** | "GPU memory (VRAM)", with VRAM in the glossary | "Not enough VRAM", with no glossary entry |
| Warnings say the consequence. | "You lose progress made since the last checkpoint." | "Warning: unsaved progress." |
| Never blame the user. **Checked** for common phrases. | "This dataset has 3 rows without an answer." | "You forgot to add answers." |
| Never say "invalid" alone. **Checked:** it needs "because" in the same sentence. | "This field has a value the app doesn't accept." | "Invalid input." |

## Errors

Every error has three parts. **Checked:** all three are required, the title is one sentence and why is one or two.

- **Title:** what happened, in one short sentence.
- **Why:** the cause, in one or two sentences.
- **What to do:** steps the user can take now. Add `windows`, `macos` or `linux` steps when they differ.

Good:

> **Training paused: the GPU ran out of memory.**
> *Why:* This model with these settings needs more GPU memory than is free.
> *What to do:* Resume with memory-saving settings. Use a smaller model.

Bad:

> **Error: CUDA OOM (exit 137).**
> *Why:* Invalid configuration.

## Commands

Put commands in backticks, like `wsl --install`.
The build counts a command as one word and skips it for acronyms.
