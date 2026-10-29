"""表情包记忆（Sticker Memory）

把 QQ 账号里的「收藏表情」变成 Bot 自己的能力：

- 自动同步收藏表情列表（emoji_id / url / 备注）
- 每次请求注入「你有一柜子表情」的自我认知，模型才知道可以主动发
- 三个工具：sticker_list 看库、sticker_send 发一个、sticker_note 写备注
- 备注和使用次数落盘，越用越清楚哪张适合什么场合

发送方式：下载表情图后按 base64 图片段发出（QQ 收藏表情没有稳定的 mface 发送接口，
图片段最通用）。
"""

from __future__ import annotations

import asyncio
import base64
import json
import random
import re
import time
from pathlib import Path

import aiohttp

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import Image, Plain
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain
from quart import jsonify, request

PLUGIN_NAME = "astrbot_plugin_sticker_memory"
MAX_DESC = 60
MAX_NOTE = 120


class StickerMemoryPlugin(Star):
    """收藏表情记忆。"""

    def __init__(self, context: Context, config: dict | None = None) -> None:
        super().__init__(context, config)
        self.config: dict = config or {}
        self._entries: dict[str, dict] = {}
        self._last_sync: float = 0.0
        self._last_send: dict[str, float] = {}
        self._data_dir = Path(StarTools.get_data_dir(PLUGIN_NAME))
        self._path = self._data_dir / "stickers.json"
        # 自动收藏的每日计数
        self._collect_day: str = ""
        self._collect_count: int = 0
        self._register_web_apis()

    # ------------------------------------------------------------------
    # 配置
    # ------------------------------------------------------------------
    def _cfg_bool(self, key: str, default: bool) -> bool:
        v = self.config.get(key, default)
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "on", "是")
        return bool(v) if v is not None else default

    def _cfg_int(self, key: str, default: int) -> int:
        try:
            return int(self.config.get(key, default))
        except (TypeError, ValueError):
            return default

    def _cfg_float(self, key: str, default: float) -> float:
        try:
            return float(self.config.get(key, default))
        except (TypeError, ValueError):
            return default

    def _cfg_str(self, key: str, default: str) -> str:
        v = self.config.get(key, default)
        return str(v) if v is not None else default

    # ------------------------------------------------------------------
    # 生命周期与存储
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        self._load()
        self.logger.info(
            "[表情记忆] 已加载 %s 个收藏表情（%s）",
            len(self._entries),
            self._path,
        )

    def _load(self) -> None:
        try:
            if self._path.exists():
                data = json.loads(self._path.read_text(encoding="utf-8"))
                if isinstance(data, dict) and isinstance(data.get("entries"), dict):
                    self._entries = data["entries"]
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[表情记忆] 读取本地表情库失败: %s", exc)

    def _save(self) -> None:
        try:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            self._path.write_text(
                json.dumps(
                    {"entries": self._entries, "updatedAt": time.time()},
                    ensure_ascii=False,
                    indent=1,
                ),
                encoding="utf-8",
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[表情记忆] 保存表情库失败: %s", exc)

    # ------------------------------------------------------------------
    # 同步
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_faces(raw) -> list:
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            for key in ("data", "faces"):
                v = raw.get(key)
                if isinstance(v, list):
                    return v
        return []

    def _apply_fetch(self, raw) -> tuple[int, int]:
        """把拉回来的列表合并进库；如果拿到的是完整列表，就删掉 QQ 里已经没有的。

        返回 (新增数, 删除数)。
        """
        items = self._parse_faces(raw)
        before = set(self._entries)
        seen: set[str] = set()
        for it in items:
            if not isinstance(it, dict):
                continue
            sid = str(
                it.get("emoji_id") or it.get("resId") or it.get("id") or ""
            ).strip()
            if not sid:
                continue
            seen.add(sid)
            old = self._entries.get(sid, {})
            self._entries[sid] = {
                "id": sid,
                "url": str(it.get("url") or old.get("url") or ""),
                "desc": str(it.get("desc") or old.get("desc") or "")[:MAX_DESC],
                "note": old.get("note", ""),
                "tags": old.get("tags", []),
                "uses": int(old.get("uses", 0)),
                "lastUsed": old.get("lastUsed", 0),
            }
        added = len(seen - before)
        removed = 0
        # 只有明确拿到完整列表时才清理，避免把"没拉全"误判成"被删了"
        if items and len(items) < 200 and seen:
            for sid in list(before - seen):
                self._entries.pop(sid, None)
                removed += 1
        self._last_sync = time.time()
        return added, removed

    def _find_bot(self):
        """找一个 aiocqhttp 连接，用于 WebUI 侧主动拉取。"""
        try:
            for inst in self.context.platform_manager.get_insts():
                bot = getattr(inst, "bot", None)
                if bot is not None:
                    return bot
        except Exception:  # noqa: BLE001
            pass
        return None

    async def _sync(self, event: AstrMessageEvent | None, force: bool = False) -> int:
        """从 QQ 拉取收藏表情；返回当前库里有多少个。"""
        if not self._cfg_bool("enable", True):
            return len(self._entries)
        if not self._cfg_bool("auto_sync", True) and not force:
            return len(self._entries)
        ttl = max(60, self._cfg_int("sync_ttl_minutes", 60) * 60)
        if not force and time.time() - self._last_sync < ttl:
            return len(self._entries)

        bot = getattr(event, "bot", None) if event is not None else None
        if bot is None:
            bot = self._find_bot()
        if bot is None:
            return len(self._entries)

        try:
            raw = await bot.call_action("fetch_custom_face_detail", count=200)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[表情记忆] 拉取收藏表情失败: %s", exc)
            return len(self._entries)

        added, removed = self._apply_fetch(raw)
        self._save()
        if added or removed:
            self.logger.info(
                "[表情记忆] 同步完成：新增 %s，清理 %s，现在共 %s 个",
                added, removed, len(self._entries),
            )
        return len(self._entries)

    # ------------------------------------------------------------------
    # 查找
    # ------------------------------------------------------------------
    def _find(self, ref: str) -> dict | None:
        ref = (ref or "").strip()
        if not ref:
            return None
        if ref in self._entries:
            return self._entries[ref]
        low = ref.lower()
        for e in self._entries.values():
            if e.get("id", "").lower() == low:
                return e
        for e in self._entries.values():
            haystack = " ".join(
                [
                    str(e.get("desc", "")),
                    str(e.get("note", "")),
                    " ".join(str(t) for t in e.get("tags", []) or []),
                ]
            ).lower()
            if low in haystack:
                return e
        return None

    async def _download(self, url: str) -> bytes | None:
        try:
            timeout = aiohttp.ClientTimeout(
                total=max(3, self._cfg_int("download_timeout", 12))
            )
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.get(url) as resp:
                    if resp.status != 200:
                        return None
                    return await resp.read()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[表情记忆] 下载表情失败: %s", exc)
            return None

    # ------------------------------------------------------------------
    # 工具：看库 / 发一个 / 写备注
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # 自动收藏：识别别人发的表情
    # ------------------------------------------------------------------
    async def _fetch_face_list(self, event: AstrMessageEvent) -> list:
        bot = getattr(event, "bot", None)
        if bot is None:
            return []
        try:
            return await bot.call_action("fetch_custom_face_detail", count=200)
        except Exception:  # noqa: BLE001
            return []

    def _collect_quota_left(self) -> bool:
        today = time.strftime("%Y-%m-%d")
        if self._collect_day != today:
            self._collect_day = today
            self._collect_count = 0
        cap = self._cfg_int("auto_collect_daily_max", 10)
        return cap <= 0 or self._collect_count < cap

    def _collect_allowed_here(self, event: AstrMessageEvent) -> bool:
        gid = str(event.get_group_id() or "")
        if not gid:
            return self._cfg_bool("auto_collect_private", True)
        groups = self._cfg_list("auto_collect_groups", [])
        return not groups or gid in groups

    def _cfg_list(self, key: str, default: list[str]) -> list[str]:
        v = self.config.get(key, default)
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        if isinstance(v, str):
            return [s.strip() for s in v.replace("，", ",").split(",") if s.strip()]
        return list(default)

    async def _add_to_favorites(
        self, event: AstrMessageEvent, image: Image, remark: str = ""
    ) -> str | None:
        """把一张图片加进 QQ 收藏表情，成功返回 emoji_id。"""
        bot = getattr(event, "bot", None)
        if bot is None:
            return None
        try:
            b64 = await image.convert_to_base64()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[表情记忆] 读取图片失败: %s", exc)
            return None
        if not b64:
            return None
        try:
            res = await bot.call_action("add_custom_face", file="base64://" + b64)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[表情记忆] 收藏失败: %s", exc)
            return None

        data = res if isinstance(res, dict) else {}
        if data.get("status") not in (None, "ok"):
            self.logger.warning("[表情记忆] 收藏被拒: %s", data.get("wording"))
            return None
        inner = data.get("data") if isinstance(data.get("data"), dict) else data
        emoji_id = str(inner.get("emoji_id") or inner.get("resId") or "").strip()
        if not emoji_id:
            self.logger.warning("[表情记忆] 收藏成功但没拿到 emoji_id")
            return None

        clean = (remark or "").strip()[:MAX_NOTE]
        if clean:
            try:
                await bot.call_action(
                    "modify_custom_face", emoji_id=emoji_id, desc=clean
                )
            except Exception:  # noqa: BLE001
                pass

        self._entries[emoji_id] = {
            "id": emoji_id,
            "url": "",
            "desc": "",
            "note": clean,
            "tags": [],
            "uses": 0,
            "lastUsed": 0,
        }
        self._collect_count += 1
        self._save()
        await self._sync(event, force=True)
        if self._cfg_bool("auto_tag_enable", False):
            entry = self._entries.get(emoji_id)
            if entry is not None and not entry.get("note"):
                if await self._vision_tag(entry):
                    self._save()
        self.logger.info(
            "[表情记忆] 已收藏表情 %s%s（今日第 %s 个）",
            emoji_id,
            f" 备注={clean}" if clean else "",
            self._collect_count,
        )
        return emoji_id

    @filter.event_message_type(
        filter.EventMessageType.ALL,
        priority=30,
    )
    async def auto_collect_stickers(self, event: AstrMessageEvent) -> None:
        """看到别人发的表情，按概率自主收藏。"""
        try:
            await self._handle_auto_collect(event)
        except Exception:  # noqa: BLE001
            self.logger.exception("[表情记忆] 自动收藏异常")

    async def _handle_auto_collect(self, event: AstrMessageEvent) -> None:
        if not self._cfg_bool("enable", True):
            return
        if not self._cfg_bool("auto_collect_enable", False):
            return
        if str(event.get_sender_id()) == str(event.get_self_id()):
            return
        if not self._collect_allowed_here(event):
            return

        images = [c for c in event.get_messages() if isinstance(c, Image)]
        if not images:
            return
        if not self._collect_quota_left():
            return
        if random.random() > float(self._cfg_float("auto_collect_probability", 0.15)):
            return

        await self._add_to_favorites(event, images[0])

    @filter.llm_tool(name="sticker_collect")
    async def sticker_collect(
        self, event: AstrMessageEvent, remark: str = ""
    ) -> str:
        """把当前这条消息里的图片/表情收藏进你自己的 QQ 表情库。

        什么时候用：群友发了一张你觉得有意思、以后想用的图/表情，就把它收进来。
        这是你自己的收藏，收完以后可以用 sticker_list 看到、用 sticker_send 发出去。

        Args:
            remark(string): 可选，给这张表情写一句备注，比如「嘲讽专用」「笑到打滚」。
        """
        images = [c for c in event.get_messages() if isinstance(c, Image)]
        if not images:
            return "这条消息里没有图片，没东西可收藏。"
        emoji_id = await self._add_to_favorites(event, images[0], remark)
        if not emoji_id:
            return "收藏失败了（可能网关不支持，或者图片读不出来）。"
        return f"已收藏进你的表情库（emoji_id={emoji_id}）。"

    # ------------------------------------------------------------------
    # 视觉打标
    # ------------------------------------------------------------------
    def _tag_provider(self):
        pid = str(self.config.get("tag_provider_id") or "").strip()
        if pid:
            p = self.context.get_provider_by_id(pid)
            if p is not None:
                return p
        try:
            return self.context.get_using_provider()
        except Exception:  # noqa: BLE001
            return None

    async def _vision_tag(self, entry: dict) -> bool:
        """让视觉模型看这张表情，写一句「画面 + 适合什么场合」。"""
        url = str(entry.get("url") or "").strip()
        if not url:
            return False
        provider = self._tag_provider()
        if provider is None:
            return False
        prompt = self._cfg_str(
            "tag_prompt",
            "这是 QQ 收藏表情包的一张图。用中文回答，只输出一行，格式："
            "画面描述|适合场合|标签1/标签2/标签3。"
            "画面描述不超过 15 字，说清图里是什么、什么表情；"
            "适合场合不超过 15 字；标签 2~3 个，每个 2~4 个字。"
            "不要客套、不要解释、不要换行。",
        )
        try:
            resp = await asyncio.wait_for(
                provider.text_chat(
                    prompt=prompt,
                    image_urls=[url],
                    session_id=f"tag-{entry['id'][:8]}",
                ),
                timeout=max(20, self._cfg_int("tag_timeout", 60)),
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[表情记忆] 打标失败 %s: %s", entry.get("id"), exc)
            return False

        text = (getattr(resp, "completion_text", "") or "").strip()
        if not text:
            return False
        parts = [p.strip() for p in re.split(r"[|｜]", text)]
        scene = parts[0] if parts and parts[0] else text
        entry["note"] = scene[:MAX_NOTE]
        if len(parts) > 2 and parts[2]:
            entry["tags"] = [
                t.strip()[:12]
                for t in re.split(r"[,，/、]", parts[2])
                if t.strip()
            ][:6]
        elif len(parts) > 1 and parts[1]:
            entry["tags"] = [
                t.strip()[:12]
                for t in re.split(r"[,，/、]", parts[1])
                if t.strip()
            ][:6]
        entry["taggedAt"] = time.time()
        return True

    async def _tag_missing(
        self, event: AstrMessageEvent, limit: int = 10
    ) -> tuple[int, int]:
        await self._sync(event)
        todo = [e for e in self._entries.values() if not e.get("note")]
        todo.sort(key=lambda e: int(e.get("uses", 0)), reverse=True)
        todo = todo[: max(1, limit)]
        done = 0
        for e in todo:
            if await self._vision_tag(e):
                done += 1
                await asyncio.sleep(0.2)
        if done:
            self._save()
        remaining = sum(1 for e in self._entries.values() if not e.get("note"))
        self.logger.info(
            "[表情记忆] 打标完成：本次 %s 张，还缺 %s 张", done, remaining
        )
        return done, remaining

    # ------------------------------------------------------------------
    # WebUI 接口
    # ------------------------------------------------------------------
    def _register_web_apis(self) -> None:
        routes = [
            ("/astrbot_plugin_sticker_memory/library", self._api_library, ["GET"], "列出收藏表情"),
            ("/astrbot_plugin_sticker_memory/save", self._api_save, ["POST"], "保存备注和标签"),
            ("/astrbot_plugin_sticker_memory/sync", self._api_sync, ["POST"], "同步收藏表情"),
            ("/astrbot_plugin_sticker_memory/tag", self._api_tag, ["POST"], "给一张表情打标"),
            ("/astrbot_plugin_sticker_memory/delete", self._api_delete, ["POST"], "从本地库删除一张表情"),
        ]
        for route, handler, methods, desc in routes:
            try:
                self.context.register_web_api(route, handler, methods, desc)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("[表情记忆] 注册接口 %s 失败: %s", route, exc)

    @staticmethod
    def _public(entry: dict) -> dict:
        return {
            "id": str(entry.get("id", "")),
            "desc": str(entry.get("desc", "")),
            "note": str(entry.get("note", "")),
            "tags": list(entry.get("tags", []) or []),
            "url": str(entry.get("url", "")),
            "uses": int(entry.get("uses", 0)),
            "lastUsed": float(entry.get("lastUsed", 0) or 0),
        }

    async def _api_library(self):
        entries = [self._public(e) for e in self._entries.values()]
        entries.sort(key=lambda e: (bool(e["note"]), e["uses"]), reverse=True)
        return jsonify(
            {
                "status": "ok",
                "count": len(entries),
                "tagged": sum(1 for e in entries if e["note"]),
                "entries": entries,
            }
        )

    async def _api_save(self):
        data = (await request.get_json(silent=True)) or {}
        sid = str(data.get("id") or "").strip()
        entry = self._entries.get(sid)
        if entry is None:
            return jsonify({"status": "error", "message": "找不到这张表情"}), 404
        if "note" in data:
            entry["note"] = str(data.get("note") or "").strip()[:MAX_NOTE]
        if "tags" in data:
            raw = data.get("tags") or []
            if isinstance(raw, str):
                raw = re.split(r"[,，/、]", raw)
            entry["tags"] = [str(t).strip()[:12] for t in raw if str(t).strip()][:8]
        self._save()
        return jsonify({"status": "ok", "entry": self._public(entry)})

    async def _api_sync(self):
        bot = self._find_bot()
        if bot is None:
            return jsonify({"status": "error", "message": "没有可用的 QQ 连接"}), 400
        try:
            raw = await bot.call_action("fetch_custom_face_detail", count=200)
        except Exception as exc:  # noqa: BLE001
            return jsonify({"status": "error", "message": f"拉取失败：{exc}"}), 500
        added, removed = self._apply_fetch(raw)
        self._save()
        return jsonify(
            {
                "status": "ok",
                "count": len(self._entries),
                "added": added,
                "removed": removed,
            }
        )

    async def _api_delete(self):
        data = (await request.get_json(silent=True)) or {}
        sid = str(data.get("id") or "").strip()
        if sid not in self._entries:
            return jsonify({"status": "error", "message": "找不到这张表情"}), 404
        self._entries.pop(sid, None)
        self._save()
        return jsonify({"status": "ok", "count": len(self._entries)})

    async def _api_tag(self):
        data = (await request.get_json(silent=True)) or {}
        sid = str(data.get("id") or "").strip()
        entry = self._entries.get(sid)
        if entry is None:
            return jsonify({"status": "error", "message": "找不到这张表情"}), 404
        ok = await self._vision_tag(entry)
        if not ok:
            return jsonify({"status": "error", "message": "打标失败（看后台日志）"}), 500
        self._save()
        return jsonify({"status": "ok", "entry": self._public(entry)})

    @filter.llm_tool(name="sticker_list")
    async def sticker_list(self, event: AstrMessageEvent, query: str = "") -> str:
        """查看你自己的 QQ 收藏表情包（这是你自己的东西，不是别人的）。

        想发表情、想知道有没有合适的表情、或者对方问「你有哪些表情」时调用。
        返回每张表情的 id、官方描述、你自己写的备注和用过几次。

        Args:
            query(string): 可选，按关键词过滤（比如「猫」「笑哭」「嘲讽」）。留空则列出全部。
        """
        await self._sync(event)
        if not self._entries:
            return "现在还没同步到收藏表情（可能 QQ 侧没有收藏，或网关不支持）。"

        items = list(self._entries.values())
        if query:
            q = query.lower()
            filtered = []
            for e in items:
                haystack = " ".join(
                    [
                        str(e.get("desc", "")),
                        str(e.get("note", "")),
                        " ".join(str(t) for t in e.get("tags", []) or []),
                    ]
                ).lower()
                if q in haystack:
                    filtered.append(e)
            items = filtered or items

        items.sort(key=lambda e: (int(e.get("uses", 0)), e.get("desc", "")), reverse=True)
        items = items[:30]
        lines = [f"你的收藏表情共 {len(self._entries)} 个，这里列出 {len(items)} 个："]
        for e in items:
            bits = [f"id={e['id']}"]
            if e.get("desc"):
                bits.append(f"官方描述={e['desc']}")
            if e.get("note"):
                bits.append(f"你的备注={e['note']}")
            if e.get("tags"):
                bits.append("标签=" + "/".join(str(t) for t in e["tags"]))
            bits.append(f"用过{e.get('uses', 0)}次")
            lines.append("- " + " | ".join(bits))
        lines.append("想发哪个就调 sticker_send，把 id 或备注关键词传进去。")
        return "\n".join(lines)

    @filter.llm_tool(name="sticker_send")
    async def sticker_send(self, event: AstrMessageEvent, sticker: str) -> str:
        """从你的 QQ 收藏表情里挑一个发到当前会话。

        什么时候用：文字表达不出的情绪（哭笑不得、无语、嘲讽、得意）、想活跃气氛、
        或者对方刚发了个表情。**不要每句话都发**，一条对话里最多一两次。

        注意：表情是单独一条消息，不能和文字拼在同一个气泡里。想说话就先正常回一句，
        再单独发表情。

        Args:
            sticker(string): 表情的 id，或者备注/描述里的关键词（比如「笑哭」「猫」）。
        """
        await self._sync(event)
        entry = self._find(sticker)
        if entry is None:
            return (
                f"没找到「{sticker}」这张表情。可以先用 sticker_list 看看你有哪些，"
                "或者传 id。"
            )

        key = event.unified_msg_origin
        cd = max(0, self._cfg_int("send_cooldown_seconds", 20))
        if cd and time.time() - self._last_send.get(key, 0.0) < cd:
            left = int(cd - (time.time() - self._last_send.get(key, 0.0)))
            return f"刚发过表情，{left} 秒后再说。先用文字回。"

        # 概率闸门：不是每次想发都能发出去
        prob = self._cfg_float("send_probability", 0.4)
        if prob <= 0:
            return "这次先不发表情，直接用文字回。"
        if prob < 1 and random.random() > prob:
            return "这次先不发表情（时机一般），直接用文字回一句。"

        url = str(entry.get("url") or "").strip()
        if not url:
            return f"表情 {entry['id']} 没有图片地址，发不出去。"

        data = await self._download(url)
        try:
            if data:
                comp = Image.fromBytes(data)
            else:
                comp = Image.fromURL(url)
            await event.send(MessageChain(chain=[comp]))
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("[表情记忆] 发表情失败: %s", exc)
            return "表情没发出去（可能是图片地址过期了）。"

        entry["uses"] = int(entry.get("uses", 0)) + 1
        entry["lastUsed"] = time.time()
        self._last_send[key] = time.time()
        self._save()
        return f"表情已发出（{entry.get('note') or entry.get('desc') or entry['id']}）。不用再描述它。"

    @filter.llm_tool(name="sticker_note")
    async def sticker_note(
        self, event: AstrMessageEvent, sticker: str, note: str
    ) -> str:
        """给一张收藏表情写备注，记下它是什么意思、什么场合用。

        别人发了一张你没收过的表情、或者你用某张表情时发现它很适合某个场景，
        都可以记一笔。备注会一直保存，以后 sticker_list 能看到。

        Args:
            sticker(string): 表情 id，或备注/描述里的关键词。
            note(string): 备注内容，简短一点，比如「嘲讽专用」「笑到打滚」。
        """
        await self._sync(event)
        entry = self._find(sticker)
        if entry is None:
            return f"没找到「{sticker}」这张表情，先用 sticker_list 确认。"
        entry["note"] = note.strip()[:MAX_NOTE]
        self._save()
        return f"记下了：{entry['id']} = {entry['note']}"

    # ------------------------------------------------------------------
    # 注入「她知道自己有表情库」
    # ------------------------------------------------------------------
    @filter.on_llm_request()
    async def inject_awareness(self, event: AstrMessageEvent, req) -> None:
        if not self._cfg_bool("enable", True):
            return
        if not self._cfg_bool("inject_awareness", True):
            return
        try:
            await self._sync(event)
        except Exception:  # noqa: BLE001
            pass
        if not self._entries:
            return

        limit = max(1, self._cfg_int("max_in_prompt", 8))
        ranked = sorted(
            self._entries.values(),
            key=lambda e: (bool(e.get("note")), int(e.get("uses", 0))),
            reverse=True,
        )[:limit]

        def _label(e: dict) -> str:
            base = (e.get("note") or e.get("desc") or e["id"])[:16]
            tags = [str(t) for t in (e.get("tags") or [])][:3]
            return f"{base}[{'/'.join(tags)}]" if tags else base

        brief = "、".join(_label(e) for e in ranked)
        text = (
            "【你的表情库】\n"
            f"你有一个自己的 QQ 收藏表情库，共 {len(self._entries)} 张，"
            "可以用工具 sticker_send 挑一张发出去（这些表情是你的，不是别人发的）。\n"
            "必须真的调用工具；不要输出 [STICKER_RECORD] 之类的文字标记来代替调用。\n"
            f"频率：大约 {int(self._cfg_float('send_probability', 0.4) * 100)}% 的回复里可以带一张表情，不要更频繁。\n"
            f"其中比较常用的：{brief}\n"
            "（格式：描述[标签/标签]，标签说明这张图适合什么场合）\n"
            "用法：情绪用文字说不到位的时侯发一张，比如无语、嘲讽、笑到不行、得意；"
            "一条对话里最多一两次，别刷屏。表情是单独一条消息，想说话就先正常回一句，"
            "再单独发表情。不要解释「我发了个表情」，也不要问对方喜不喜欢。"
        )
        try:
            from astrbot.core.agent.message import TextPart

            req.extra_user_content_parts.append(
                TextPart(text=text).mark_as_temp()
            )
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # 发送前清洗：模型偶尔会把提示词结构照抄出来
    # ------------------------------------------------------------------
    _RE_SYSTEM_TAG = re.compile(r"</?system_reminder\s*>", re.IGNORECASE)
    _RE_SYSTEM_BLOCK = re.compile(
        r"<system_reminder>.*?</system_reminder>", re.IGNORECASE | re.DOTALL
    )
    _RE_ROLE_LINE = re.compile(
        r"(?m)^[ \t]*(?:user|assistant|system)[ \t]*[:：]?[ \t]*", re.IGNORECASE
    )
    # 伪工具调用：<tool_call>...</tool_call> 整段
    _RE_TOOL_BLOCK = re.compile(r"<tool_call>.*?</tool_call>", re.IGNORECASE | re.DOTALL)
    # 整行的伪标记：[STICKER_RECORD] use sticker "骄傲" / [TOOL_CALL] xxx
    _RE_FAKE_MARKER_LINE = re.compile(
        r"(?m)^[ \t]*\[[A-Z][A-Z0-9_]{2,}\][^\n]*\n?", 
    )
    # 残句：use sticker "..." / call tool ...
    _RE_FAKE_CALL_LINE = re.compile(
        r"(?m)^[ \t]*(?:use|call|invoke)\s+(?:sticker|tool)[^\n]*\n?", re.IGNORECASE
    )

    @classmethod
    def _scrub(cls, text: str) -> str:
        if not text:
            return text
        out = cls._RE_SYSTEM_BLOCK.sub("", text)
        out = cls._RE_TOOL_BLOCK.sub("", out)
        out = cls._RE_SYSTEM_TAG.sub("", out)
        out = cls._RE_ROLE_LINE.sub("", out)
        out = cls._RE_FAKE_MARKER_LINE.sub("", out)
        out = cls._RE_FAKE_CALL_LINE.sub("", out)
        # 收拾残留：连续空行压成一个，首尾空白去掉
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out.strip()

    @filter.on_decorating_result()
    async def scrub_prompt_leaks(self, event: AstrMessageEvent) -> None:
        try:
            result = event.get_result()
            if result is None:
                return
            chain = getattr(result, "chain", None)
            if not chain:
                return
            changed = False
            cleaned = []
            for comp in chain:
                if isinstance(comp, Plain):
                    new_text = self._scrub(comp.text)
                    if new_text != comp.text:
                        changed = True
                        self.logger.warning(
                            "[表情记忆] 清洗了回复里的提示词残留: %s",
                            comp.text[:60].replace("\n", " "),
                        )
                    if new_text.strip():
                        cleaned.append(Plain(new_text))
                    continue
                cleaned.append(comp)
            if changed:
                result.chain = cleaned
        except Exception:  # noqa: BLE001
            self.logger.exception("[表情记忆] 清洗回复失败")

    # ------------------------------------------------------------------
    # 指令
    # ------------------------------------------------------------------
    @filter.command("表情")
    async def sticker_cmd(self, event: AstrMessageEvent):
        """表情库：查看 / 同步 / 打标 / 标签 / 自动收藏"""
        raw = (event.message_str or "").strip()
        body = raw
        for p in ("/表情", "表情"):
            if body.startswith(p):
                body = body[len(p):].strip()
                break
        tokens = body.split()
        sub = tokens[0].lower() if tokens else ""

        if sub in ("同步", "sync", "刷新"):
            n = await self._sync(event, force=True)
            yield event.plain_result(f"已同步，收藏表情 {n} 个。")
            return

        if sub in ("打标", "tag"):
            limit = int(tokens[1]) if len(tokens) > 1 and tokens[1].isdigit() else 10
            if not self._tag_provider():
                yield event.plain_result("没有可用的视觉模型，打标失败。")
                return
            done, left = await self._tag_missing(event, limit)
            yield event.plain_result(
                f"打标完成：本次 {done} 张，还缺 {left} 张没描述。"
            )
            return

        if sub in ("删除", "del", "delete"):
            if len(tokens) < 2:
                yield event.plain_result(
                    "用法：/表情 删除 <id或关键词>（只删插件里的记录，QQ 收藏要在手机/电脑上删）"
                )
                return
            entry = self._find(" ".join(tokens[1:]))
            if entry is None:
                yield event.plain_result("没找到这张表情。")
                return
            self._entries.pop(entry["id"], None)
            self._save()
            yield event.plain_result(
                f"已从插件库里移除 {entry['id']}（QQ 里的收藏不受影响）。"
            )
            return

        if sub in ("标签", "分类", "tags"):
            if len(tokens) < 3:
                yield event.plain_result("用法：/表情 标签 <id或关键词> <标签1,标签2>")
                return
            ref = tokens[1]
            tags = [t.strip()[:12] for t in re.split(r"[,，/、]", " ".join(tokens[2:])) if t.strip()][:8]
            entry = self._find(ref)
            if entry is None:
                yield event.plain_result(f"没找到「{ref}」，先发 /表情 看看库里有什么。")
                return
            entry["tags"] = tags
            self._save()
            show = "/".join(tags) if tags else "（已清空）"
            yield event.plain_result(f"{entry['id']} 的标签：{show}")
            return

        if "自动收藏" in body:
            on = any(t in body for t in ("开", "on", "启用"))
            off = any(t in body for t in ("关", "off", "禁用"))
            if on or off:
                self.config["auto_collect_enable"] = bool(on)
                yield event.plain_result(
                    "自动收藏已开启（看到别人的表情会按概率收进你的库）。"
                    if on
                    else "自动收藏已关闭。"
                )
                return
            state = "开" if self._cfg_bool("auto_collect_enable", False) else "关"
            yield event.plain_result(f"自动收藏：{state}")
            return

        await self._sync(event)
        if not self._entries:
            yield event.plain_result("还没有同步到收藏表情。")
            return
        noted = [e for e in self._entries.values() if e.get("note")]
        tagged = [e for e in self._entries.values() if e.get("tags")]
        lines = [
            f"收藏表情 {len(self._entries)} 个：{len(noted)} 个有描述，{len(tagged)} 个有标签。",
        ]
        for e in list(self._entries.values())[:10]:
            label = e.get("note") or e.get("desc") or "（无描述）"
            tag = (" / " + "/".join(e["tags"])) if e.get("tags") else ""
            lines.append(f"- {e['id'][:12]}… {label}{tag}（用过 {e.get('uses', 0)} 次）")
        lines.append("指令：/表情 同步 | /表情 打标 [数量] | /表情 标签 <id或词> <标签>")
        lines.append("也可以去 WebUI 插件页的「表情包」页面点着改。")
        yield event.plain_result("\n".join(lines))

    async def terminate(self) -> None:
        self._save()
