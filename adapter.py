"""Photon Plus adapter: thin subclass of upstream PhotonAdapter.

Strategy (avoids COPY drift):
- Reuse everything from upstream: auth, sidecar lifecycle, inbound gRPC
  stream, media, reactions. We import PhotonAdapter at runtime and only
  override two behaviors below.
- If upstream refactors (rename/remove), this import fails loudly at load
  time instead of silently delivering stale behavior. See UPSTREAM_PIN.

Override 1 — chunked outbound sends:
- Upstream caps at 8000 and hard-cuts in _sidecar_send. Gateway delivery
  caps non-chunking adapters at 4000 (gateway/delivery.py
  MAX_PLATFORM_OUTPUT) and truncates with a footer. Setting
  splits_long_messages = True tells the gateway to hand us the full
  payload, and our send() splits it into sequential iMessage bubbles.

Override 2 — swipe-reply unwrap (stub):
- Upstream _dispatch_inbound only handles text/attachment/voice/group/
  reaction. A swipe-reply arrives as content type "reply" and falls into
  the "[Photon content type not handled: reply]" placeholder with the body
  lost. _normalize_reply_event rewrites reply-shaped events into
  text-shaped events (preserving reply_to ids) before delegating to
  super()._dispatch_inbound. Unknown reply shapes are logged with keys so
  the first real payload can be captured from logs and the unwrapper
  tightened. Threading itself is not possible: Spectrum exposes no thread
  API (upstream notes "Spectrum has no threads yet"), so this preserves
  quote context only.
"""

from __future__ import annotations

import copy
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Chunk size for outbound bubbles. Conservative vs the 8000 sidecar cap so
# each bubble stays comfortably inside both the gateway and sidecar limits.
PLUS_CHUNK_SIZE = 3500

try:
    # Installed layout: /opt/hermes/plugins/platforms/photon/adapter.py
    from plugins.platforms.photon.adapter import (
        PhotonAdapter,
        check_requirements,
        validate_config,
    )
except ImportError:
    try:
        # Fallback: some installs expose it under the gateway package.
        from gateway.plugins.platforms.photon.adapter import (  # type: ignore
            PhotonAdapter,
            check_requirements,
            validate_config,
        )
    except ImportError as exc:
        raise ImportError(
            "photon-plus requires the upstream photon plugin "
            "(plugins.platforms.photon.adapter.PhotonAdapter). "
            "Check UPSTREAM_PIN for the pinned upstream commit."
        ) from exc


def _split_chunks(text: str, limit: int = PLUS_CHUNK_SIZE) -> List[str]:
    text = text or ""
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    rest = text
    while len(rest) > limit:
        cut = rest.rfind("\n\n", 0, limit)
        if cut < 0:
            cut = rest.rfind("\n", 0, limit)
        if cut < 0:
            cut = rest.rfind(" ", 0, limit)
        if cut <= 0:
            cut = limit
        chunks.append(rest[:cut].rstrip())
        rest = rest[cut:].lstrip()
    if rest:
        chunks.append(rest)
    return [c for c in chunks if c]


def _first_text(*candidates: Any) -> str:
    for c in candidates:
        if isinstance(c, str) and c.strip():
            return c
        if isinstance(c, dict):
            t = c.get("text") or c.get("body") or c.get("message")
            if isinstance(t, str) and t.strip():
                return t
    return ""


def _normalize_reply_event(event: Dict[str, Any]) -> bool:
    """Rewrite a reply-shaped inbound event into a text-shaped one.

    Returns True when the event was recognized as a reply and rewritten
    in place; False when it is not a reply shape (caller passes through).
    Unknown reply shapes are logged with their keys for payload capture.
    """
    content = event.get("content")
    if not isinstance(content, dict):
        return False
    if content.get("type") != "reply":
        return False

    # Common shapes seen across Spectrum-style payloads. The exact reply
    # schema is unconfirmed until the first real payload is captured, so
    # probe several nests before giving up.
    reply_body = _first_text(
        content.get("text"),
        content.get("body"),
        content.get("message"),
        (content.get("reply") or {}),
        (content.get("quoted") or {}),
        (content.get("original") or {}),
    )
    if not reply_body:
        # Group-wrapped reply: items[0].content carries the real payload.
        for item in content.get("items") or []:
            if not isinstance(item, dict):
                continue
            inner = item.get("content") or {}
            t = _first_text(inner.get("text"), inner.get("body"), inner)
            if t:
                reply_body = t
                break

    if not reply_body:
        logger.warning(
            "[photon-plus] unknown reply shape, keys=%s nested=%s",
            sorted(content.keys()),
            sorted(
                k for v in content.values() if isinstance(v, dict) for k in v.keys()
            )[:20],
        )
        return False

    target_id = (
        content.get("targetMessageId")
        or content.get("replyToMessageId")
        or (content.get("reply") or {}).get("messageId")
        if isinstance(content.get("reply"), dict)
        else None
    )
    target_text = _first_text(
        content.get("targetText"),
        content.get("quotedText"),
        (content.get("quoted") or {}),
    )
    new_content: Dict[str, Any] = {"type": "text", "text": reply_body}
    if target_id:
        new_content["targetMessageId"] = target_id
    if target_text:
        new_content["targetText"] = target_text
    event["content"] = new_content
    return True


class PhotonPlusAdapter(PhotonAdapter):
    """Upstream adapter + chunked sends + reply unwrap."""

    # Tells gateway/delivery.py to hand us the full payload instead of
    # truncating at MAX_PLATFORM_OUTPUT with a footer.
    splits_long_messages = True

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        text = self.format_message(content) if hasattr(self, "format_message") else content
        chunks = _split_chunks(text)
        result = None
        for i, chunk in enumerate(chunks):
            # Only the first bubble carries reply_to to avoid N notifications.
            result = await super().send(
                chat_id,
                chunk,
                reply_to=reply_to if i == 0 else None,
                metadata=metadata,
            )
            if not getattr(result, "success", False):
                logger.warning(
                    "[photon-plus] chunk %d/%d failed: %s",
                    i + 1, len(chunks), getattr(result, "error", "?"),
                )
                break
        return result

    async def _dispatch_inbound(self, event: Dict[str, Any]) -> None:
        try:
            # Work on a copy so unknown shapes still reach upstream intact
            # for their placeholder path if we fail to normalize.
            evt = copy.deepcopy(event)
            if _normalize_reply_event(evt):
                await super()._dispatch_inbound(evt)
                return
        except Exception as exc:
            logger.warning("[photon-plus] reply normalize failed: %s", exc)
        await super()._dispatch_inbound(event)


def _env_enablement() -> Optional[dict]:
    """Seed PlatformConfig.extra from env (mirrors upstream's shape).

    The special ``home_channel`` key is handled by the core plugin hook
    and becomes a proper ``HomeChannel`` on ``PlatformConfig``.
    """
    project_id = os.getenv("PHOTON_PROJECT_ID", "").strip()
    project_secret = os.getenv("PHOTON_PROJECT_SECRET", "").strip()
    if not (project_id and project_secret):
        return None
    seed: Dict[str, Any] = {
        "project_id": project_id,
        "project_secret": project_secret,
    }
    home = (os.getenv("PHOTON_PLUS_HOME_CHANNEL", "").strip()
            or os.getenv("PHOTON_HOME_CHANNEL", "").strip())
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("PHOTON_PLUS_HOME_CHANNEL_NAME", "Home"),
        }
    return seed


def register(ctx) -> None:
    """Called by the Hermes plugin loader at startup."""
    ctx.register_platform(
        name="photon-plus",
        label="iMessage via Photon Plus",
        adapter_factory=lambda cfg: PhotonPlusAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["PHOTON_PROJECT_ID", "PHOTON_PROJECT_SECRET"],
        install_hint=(
            "Run upstream setup once: hermes photon setup "
            "(photon-plus reuses PHOTON_PROJECT_ID/SECRET and the sidecar)."
        ),
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="PHOTON_PLUS_HOME_CHANNEL",
        allowed_users_env="PHOTON_ALLOWED_USERS",
        allow_all_env="PHOTON_ALLOW_ALL_USERS",
        max_message_length=PLUS_CHUNK_SIZE,
        emoji="\U0001f4f1",
        pii_safe=True,
        allow_update_command=True,
        platform_hint=(
            "You are communicating via Photon Plus (iMessage). "
            "Treat replies like regular text messages. Keep replies short. "
            "Recipient identifiers are E.164 phone numbers; never expose "
            "them unless asked."
        ),
    )
