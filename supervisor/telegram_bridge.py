"""
Ouroboros — Telegram Bridge

Polls Telegram for incoming messages and feeds them into the agent's message bus.
Agent replies are sent back to the Telegram chat.

Owner chat_id is auto-learned from the first message received and persisted in state.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

import requests

log = logging.getLogger(__name__)

_POLL_TIMEOUT = 30
_RETRY_SLEEP = 5


class TelegramBridge:
    def __init__(self, token: str, local_bridge, load_state_fn, save_state_fn):
        self.token = token
        self.bridge = local_bridge
        self._load_state = load_state_fn
        self._save_state = save_state_fn
        self._base = f"https://api.telegram.org/bot{token}"
        self._offset = 0
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._owner_chat_id: Optional[int] = self._restore_chat_id()

    def _restore_chat_id(self) -> Optional[int]:
        try:
            st = self._load_state()
            cid = st.get("telegram_owner_chat_id")
            if cid:
                log.info("Telegram: restored owner_chat_id=%s", cid)
                return int(cid)
        except Exception:
            pass
        return None

    def _persist_chat_id(self, chat_id: int) -> None:
        try:
            st = self._load_state()
            st["telegram_owner_chat_id"] = chat_id
            self._save_state(st)
        except Exception as e:
            log.warning("Telegram: failed to persist chat_id: %s", e)

    def start(self) -> None:
        if self._running:
            return
        self._running = True

        _original_broadcast = self.bridge._broadcast_fn

        def _wrapped_broadcast(msg: dict) -> None:
            if _original_broadcast:
                _original_broadcast(msg)
            if msg.get("type") == "chat" and msg.get("role") == "assistant":
                text = msg.get("content", "").strip()
                if text:
                    self._send(text)

        self.bridge._broadcast_fn = _wrapped_broadcast

        self._thread = threading.Thread(target=self._poll_loop, daemon=True, name="telegram-poll")
        self._thread.start()
        log.info("Telegram bridge started (owner_chat_id=%s)", self._owner_chat_id)

        if self._owner_chat_id:
            self._send("✅ Ouroboros is online and connected to Telegram.")

    def stop(self) -> None:
        self._running = False
        log.info("Telegram bridge stopped.")

    def _poll_loop(self) -> None:
        while self._running:
            try:
                updates = self._get_updates()
                for upd in updates:
                    self._offset = upd["update_id"] + 1
                    self._handle_update(upd)
            except Exception as e:
                log.warning("Telegram poll error: %s", e)
                time.sleep(_RETRY_SLEEP)

    def _get_updates(self) -> list:
        r = requests.get(
            f"{self._base}/getUpdates",
            params={"offset": self._offset, "timeout": _POLL_TIMEOUT},
            timeout=_POLL_TIMEOUT + 5,
        )
        r.raise_for_status()
        return r.json().get("result", [])

    def _handle_update(self, upd: dict) -> None:
        msg = upd.get("message") or upd.get("edited_message")
        if not msg:
            return

        text = (msg.get("text") or "").strip()
        chat_id = msg["chat"]["id"]

        if self._owner_chat_id is None:
            self._owner_chat_id = chat_id
            self._persist_chat_id(chat_id)
            log.info("Telegram: learned owner_chat_id=%s", chat_id)

        if chat_id != self._owner_chat_id:
            log.debug("Telegram: ignoring message from non-owner chat_id=%s", chat_id)
            return

        if not text:
            return

        log.info("Telegram → agent: %s", text[:80])
        self.bridge.ui_send(text)

    def _send(self, text: str) -> None:
        if not self._owner_chat_id:
            return
        for chunk in _split(text, 4000):
            try:
                requests.post(
                    f"{self._base}/sendMessage",
                    json={"chat_id": self._owner_chat_id, "text": chunk},
                    timeout=10,
                )
            except Exception as e:
                log.warning("Telegram send error: %s", e)


def _split(text: str, max_len: int) -> list:
    if len(text) <= max_len:
        return [text]
    chunks = []
    while text:
        chunks.append(text[:max_len])
        text = text[max_len:]
    return chunks
