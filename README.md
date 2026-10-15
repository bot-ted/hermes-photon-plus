# hermes-photon-plus

Fork-style Hermes platform plugin for iMessage via Photon Spectrum.

Thin subclass of the upstream `photon` adapter — reuses its auth, Node
sidecar, and inbound stream, adds two behaviors:

1. Chunked outbound sends (`splits_long_messages = True`, 3500-char
   bubbles) so cron briefs arrive whole instead of truncated with a footer.
2. Swipe-reply unwrap: reply-shaped inbound events are rewritten to text
   events before dispatch. True threading is not possible — Spectrum
   exposes no thread API — so this preserves quote context only.

## Layout

plugin.yaml — platform manifest (kind platform, env declarations)
adapter.py — PhotonPlusAdapter + register()
__init__.py — import-light entry point
UPSTREAM_PIN — pinned upstream commit this was built against

## Install (dev)

hermes plugins install ./ — local path install
hermes plugins enable photon-plus
hermes plugins doctor photon-plus
hermes plugins validate ./ --install-deps

Set PHOTON_PLUS_HOME_CHANNEL or reuse PHOTON_HOME_CHANNEL for cron
delivery. Upstream setup only needs to run once: hermes photon setup.

## Upstream tracking

See UPSTREAM_PIN. When upstream photon/adapter.py changes, re-validate
with doctor and bump the pin. The import fails loudly if the base class
moves, by design — no silent COPY drift.
