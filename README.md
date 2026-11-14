# MasterRL

An open-source desktop app for training AI models on your own GPUs.

**Status:** Planning complete. Implementation starts with Wave 1.

## What it will do

- Fine-tune models (SFT, LoRA) on a laptop GPU, with checkpoints you can resume.
- Run RL and RLHF (GRPO, DPO, PPO) with reward tools and safety monitors.
- Evaluate and export models.
- Train across several of your machines (LAN, SSH, Tailscale, Cloudflare).
- Work on NVIDIA (Windows via WSL2, Linux) and Apple Silicon (MLX).
- Explain everything in plain language; nothing is hosted by the project.

## Where things are

| What | Where |
|------|-------|
| Architecture index | [docs/architecture/README.md](docs/architecture/README.md) |
| Product shape (master doc) | [Doc 22](docs/architecture/22-desktop-product-architecture.md) |
| Build plan (10 waves, 50 parts) | [docs/implementation/README.md](docs/implementation/README.md) |
| Rules for AI agents | [AGENTS.md](AGENTS.md) |
| Current state | [memory.md](memory.md) |

## License

Apache-2.0. See [LICENSE](LICENSE).
