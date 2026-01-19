from __future__ import annotations

import asyncio
import json
import random
import re
import html
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from jinja2 import Environment, FileSystemLoader
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
import astrbot.api.message_components as Comp
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
from astrbot.api.all import *

try:
    from astrbot.api import AstrBotConfig  # type: ignore
except Exception:  # pragma: no cover
    AstrBotConfig = dict  # type: ignore


PLUGIN_NAME = "quotes"


@dataclass
class Quote:
    id: str
    qq: str
    name: str
    text: str
    created_by: str
    created_at: float
    images: List[str] = field(default_factory=list)
    videos: List[str] = field(default_factory=list)
    group: str = ""


class QuoteStore:
    """简易 JSON 存储,保存于插件专属数据目录（data/plugin_data/quotes/）。"""

    def __init__(self, root_data_dir: Path, http_client: Optional[Any] = None):
        self.root = Path(root_data_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.images_dir = self.root / "images"
        self.images_dir.mkdir(parents=True, exist_ok=True)
        self.videos_dir = self.root / "videos"
        self.videos_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir = self.root / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.file = self.root / "quotes.json"
        self._http = http_client
        self._lock = asyncio.Lock()
        if not self.file.exists():
            self._write({"quotes": []})
        try:
            self._quotes: List[Dict[str, Any]] = self._read().get("quotes", [])
        except Exception:
            self._quotes = []

    def images_rel(self, filename: str, group_key: Optional[str] = None) -> str:
        if group_key:
            return f"images/{group_key}/{filename}"
        return f"images/{filename}"

    def images_abs(self, filename: str, group_key: Optional[str] = None) -> Path:
        base = self.images_dir / group_key if group_key else self.images_dir
        base.mkdir(parents=True, exist_ok=True)
        return base / filename

    def videos_rel(self, filename: str, group_key: Optional[str] = None) -> str:
        if group_key:
            return f"videos/{group_key}/{filename}"
        return f"videos/{filename}"

    def videos_abs(self, filename: str, group_key: Optional[str] = None) -> Path:
        base = self.videos_dir / group_key if group_key else self.videos_dir
        base.mkdir(parents=True, exist_ok=True)
        return base / filename

    async def save_image_from_url(self, url: str, group_key: Optional[str] = None) -> Optional[str]:
        try:
            if self._http is None:
                import httpx
                async with httpx.AsyncClient(timeout=20) as client:
                    resp = await client.get(url)
                    content = resp.content
                    ct = (resp.headers.get("Content-Type") or "").lower()
            else:
                resp = await self._http.get(url)
                content = resp.content
                ct = (resp.headers.get("Content-Type") or "").lower()
            ext = ".jpg"
            if "png" in ct:
                ext = ".png"
            elif "webp" in ct:
                ext = ".webp"
            elif "gif" in ct:
                ext = ".gif"
            from urllib.parse import urlparse
            p = urlparse(url)
            name_guess = Path(p.path).name
            if "." in name_guess and len(Path(name_guess).suffix) <= 5:
                ext = Path(name_guess).suffix
            from time import time
            filename = f"{int(time()*1000)}_{random.randint(1000,9999)}{ext}"
            abs_path = self.images_abs(filename, group_key)
            abs_path.write_bytes(content)
            return self.images_rel(filename, group_key)
        except Exception as e:
            logger.warning(f"下载图片失败: {e}")
            return None

    async def save_image_from_fs(self, src: str, group_key: Optional[str] = None) -> Optional[str]:
        try:
            sp = Path(src)
            if not sp.exists():
                return None
            ext = sp.suffix or ".jpg"
            from time import time
            filename = f"{int(time()*1000)}_{random.randint(1000,9999)}{ext}"
            dp = self.images_abs(filename, group_key)
            data = sp.read_bytes()
            dp.write_bytes(data)
            return self.images_rel(filename, group_key)
        except Exception as e:
            logger.warning(f"保存本地图片失败: {e}")
            return None

    async def save_video_from_url(self, url: str, group_key: Optional[str] = None) -> Optional[str]:
        try:
            if self._http is None:
                import httpx
                async with httpx.AsyncClient(timeout=60) as client:  # 视频可能较大，超时时间延长
                    resp = await client.get(url)
                    content = resp.content
                    ct = (resp.headers.get("Content-Type") or "").lower()
            else:
                resp = await self._http.get(url)
                content = resp.content
                ct = (resp.headers.get("Content-Type") or "").lower()
            # 根据常见视频扩展名确定后缀
            ext = ".mp4"
            if "webm" in ct:
                ext = ".webm"
            elif "quicktime" in ct or "mov" in ct:
                ext = ".mov"
            elif "avi" in ct:
                ext = ".avi"
            elif "flv" in ct:
                ext = ".flv"
            elif "mkv" in ct:
                ext = ".mkv"
            # 尝试从URL路径推断扩展名
            from urllib.parse import urlparse
            p = urlparse(url)
            name_guess = Path(p.path).name
            if "." in name_guess and len(Path(name_guess).suffix) <= 6:
                ext = Path(name_guess).suffix
            from time import time
            filename = f"{int(time()*1000)}_{random.randint(1000,9999)}{ext}"
            abs_path = self.videos_abs(filename, group_key)
            abs_path.write_bytes(content)
            return self.videos_rel(filename, group_key)
        except Exception as e:
            logger.warning(f"下载视频失败: {e}")
            return None

    async def save_video_from_fs(self, src: str, group_key: Optional[str] = None) -> Optional[str]:
        try:
            sp = Path(src)
            if not sp.exists():
                return None
            ext = sp.suffix or ".mp4"
            from time import time
            filename = f"{int(time()*1000)}_{random.randint(1000,9999)}{ext}"
            dp = self.videos_abs(filename, group_key)
            data = sp.read_bytes()
            dp.write_bytes(data)
            return self.videos_rel(filename, group_key)
        except Exception as e:
            logger.warning(f"保存本地视频失败: {e}")
            return None

    def _read(self) -> Dict[str, Any]:
        if not self.file.exists():
            return {"quotes": []}
        try:
            return json.loads(self.file.read_text(encoding="utf-8"))
        except Exception:
            logger.error("quotes.json 解析失败，已回退为空列表。")
            return {"quotes": []}

    def _write(self, data: Dict[str, Any]):
        self.file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    async def add(self, q: Quote):
        async with self._lock:
            self._quotes.append(asdict(q))
            self._write({"quotes": self._quotes})

    async def random_one(self, group_key: Optional[str]) -> Optional[Quote]:
        if group_key is None:
            arr = self._quotes
        else:
            arr = [x for x in self._quotes if str(x.get("group") or "") == str(group_key)]
        if not arr:
            return None
        obj = random.choice(arr)
        return Quote(**obj)

    async def random_one_by_qq(self, qq: str, group_key: Optional[str]) -> Optional[Quote]:
        if group_key is None:
            arr = [x for x in self._quotes if str(x.get("qq") or "") == str(qq)]
        else:
            arr = [x for x in self._quotes if str(x.get("qq") or "") == str(qq) and str(x.get("group") or "") == str(group_key)]
        if not arr:
            return None
        obj = random.choice(arr)
        return Quote(**obj)

    async def get_by_id(self, qid: str) -> Optional[Quote]:
        """根据 ID 获取语录"""
        for x in self._quotes:
            if str(x.get("id")) == str(qid):
                return Quote(**x)
        return None

    async def delete_by_id(self, qid: str) -> bool:
        """删除语录并清理关联图片"""
        async with self._lock:
            # 查找要删除的语录
            target_quote = None
            for x in self._quotes:
                if str(x.get("id")) == str(qid):
                    target_quote = x
                    break
            
            if not target_quote:
                return False
            
            # 删除关联图片
            images = target_quote.get("images") or []
            for img_rel in images:
                try:
                    p = Path(img_rel)
                    img_path = p if p.is_absolute() else (self.root / img_rel)
                    if img_path.exists():
                        img_path.unlink()
                        logger.info(f"已删除图片: {img_path}")
                except Exception as e:
                    logger.warning(f"删除图片失败 {img_rel}: {e}")
            
            # 删除关联视频
            videos = target_quote.get("videos") or []
            for video_rel in videos:
                try:
                    p = Path(video_rel)
                    video_path = p if p.is_absolute() else (self.root / video_rel)
                    if video_path.exists():
                        video_path.unlink()
                        logger.info(f"已删除视频: {video_path}")
                except Exception as e:
                    logger.warning(f"删除视频失败 {video_rel}: {e}")
            
            # 删除语录记录
            self._quotes = [x for x in self._quotes if str(x.get("id")) != str(qid)]
            self._write({"quotes": self._quotes})
            return True


@register(
    PLUGIN_NAME,
    "Codex",
    "提交语录并生成带头像的语录图片",
    "1.0.0",
    "https://example.com/astrbot-plugin-quotes",
)
class QuotesPlugin(Star):
    def __init__(self, context: Context, config: Optional[AstrBotConfig] = None):
        super().__init__(context)
        self.config = config or {}
        storage = str(self.config.get("storage") or "").strip()
        data_root = Path(storage) if storage else (Path.cwd() / "data" / "plugin_data" / PLUGIN_NAME)
        try:
            import httpx  # type: ignore
            self.http_client = httpx.AsyncClient(timeout=20)
        except Exception:
            self.http_client = None
        self.store = QuoteStore(data_root, http_client=self.http_client)
        self.img_cfg = (self.config.get("image") or {})
        self.perf_cfg = (self.config.get("performance") or {})
        self._cfg_text_mode = bool(self.perf_cfg.get("text_mode", False))
        self._cfg_render_cache = bool(self.perf_cfg.get("render_cache", True))
        self._cfg_cache_retention = int(self.perf_cfg.get("cache_retention_days", 3))
        self._cfg_global_mode = bool(self.config.get("global_mode", False))

        # 新增：戳一戳配置
        self.poke_cfg = (self.config.get("poke") or {})
        self._cfg_poke_enabled = bool(self.poke_cfg.get("enabled", True))
        self._cfg_poke_probability = float(self.poke_cfg.get("probability", 0.3))
        self._cfg_poke_whitelist = set(map(str, self.poke_cfg.get("group_whitelist", [])))

        self._pending_qid: Dict[str, str] = {}
        self._last_sent_qid: Dict[str, str] = {}
        
        # 初始化 Jinja2 环境
        template_dir = Path(__file__).parent / "templates"
        template_dir.mkdir(exist_ok=True) # 确保目录存在
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=True
        )

        # Playwright 浏览器实例（延迟初始化）
        self._browser = None
        self._playwright = None

    async def initialize(self):
        """异步初始化 Playwright。"""
        try:
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)
            logger.info("Playwright 初始化成功")
            
            # 启动时清理过期缓存
            if self._cfg_cache_retention > 0:
                await self._clean_expired_cache()
                
        except ImportError:
            logger.warning("Playwright 未安装，将回退到框架自带渲染方法")
        except Exception as e:
            logger.warning(f"Playwright 初始化失败: {e}")

    async def add_quote(self, event: AstrMessageEvent, uid: str = ""):
        """添加语录（上传）"""
        reply_msg_id = self._get_reply_message_id(event)
        
        # 修复：检查是否回复了消息
        if not reply_msg_id:
            yield event.plain_result("请先回复某人的消息，并发送「丢出去」来保存语录。")
            return
        
        target_text: Optional[str] = None
        target_qq: Optional[str] = None
        target_name: Optional[str] = None
        ret: Dict[str, Any] = await self._fetch_onebot_msg(event, reply_msg_id) if reply_msg_id else {}
        if ret:
            target_text = self._extract_plaintext_from_onebot_message(ret.get("message"))
            sender = ret.get("sender") or {}
            target_qq = str(sender.get("user_id") or sender.get("qq") or "") or None
            card = (sender.get("card") or "").strip()
            nickname = (sender.get("nickname") or "").strip()
            target_name = card or nickname or target_qq

        if not target_text:
            text = (event.message_str or "").strip()
            for kw in ("上传", "丢出去"):
                if text.startswith(kw):
                    text = text[len(kw):].strip()
            target_text = text or None

        group_key = str(event.get_group_id() or f"private_{event.get_sender_id()}")
        images_from_reply: List[str] = []
        videos_from_reply: List[str] = []
        if event.get_platform_name() == "aiocqhttp" and ret:
            images_from_reply = await self._ingest_images_from_onebot_message(event, ret.get("message"), group_key)
            videos_from_reply = await self._ingest_videos_from_onebot_message(event, ret.get("message"), group_key)
        images_from_current: List[str] = await self._ingest_images_from_segments(event, group_key)
        videos_from_current: List[str] = await self._ingest_videos_from_segments(event, group_key)
        images: List[str] = list(dict.fromkeys(images_from_reply + images_from_current))
        videos: List[str] = list(dict.fromkeys(videos_from_reply + videos_from_current))

        target_text = self._strip_at_tokens(target_text or "") or ""

        if not target_text and not images and not videos:
            yield event.plain_result("未获取到被回复消息内容或图片/视频，请确认已正确回复对方的消息或附带图片/视频。")
            return
        if not target_text and images:
            target_text = "[图片]"
        if not target_text and videos:
            target_text = "[视频]"

        param_qq = uid.strip() if uid and uid.strip().isdigit() and len(uid.strip()) >= 5 else ""
        mention_qq = self._extract_at_qq(event)
        
        if param_qq:
            target_qq = str(param_qq)
        elif mention_qq:
            target_qq = str(mention_qq)
        elif images_from_current:
            target_qq = str(event.get_sender_id())
        else:
            target_qq = str(target_qq or "")

        target_name = await self._resolve_user_name(event, target_qq) if target_qq else ""
        if not target_name:
            target_name = target_qq or "未知用户"

        from time import time
        q = Quote(
            id=str(int(time()*1000)) + f"_{random.randint(1000,9999)}",
            qq=str(target_qq or ""),
            name=str(target_name),
            text=str(target_text),
            created_by=str(event.get_sender_id()),
            created_at=time(),
            images=images,
            videos=videos,
            group=group_key,
        )
        await self.store.add(q)
        if images and videos:
             yield event.plain_result(f"已经帮你丢出去 {q.name} 的语录，并保存 {len(images)} 张图片和 {len(videos)} 个视频了哦~")
        elif images:
            yield event.plain_result(f"已经帮你丢出去 {q.name} 的语录，并保存 {len(images)} 张图片了哦~")
        elif videos:
            yield event.plain_result(f"已经帮你丢出去 {q.name} 的语录，并保存 {len(videos)} 个视频了哦~")
        else:
            yield event.plain_result(f"已经帮你丢出去 {q.name} 的语录：{target_text} 了哦~")

    @event_message_type(EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):

        #region 戳一戳检测
        # 使用代码2中的方式：通过 message_obj.raw_message
        message_obj = getattr(event, "message_obj", None)
        if message_obj:
            raw_message = getattr(message_obj, "raw_message", {}) or {}
        else:
            raw_message = {}

        # 使用代码2中的条件判断
        if raw_message.get('post_type') == 'notice' and \
                raw_message.get('notice_type') == 'notify' and \
                raw_message.get('sub_type') == 'poke':
            bot_id = raw_message.get('self_id')
            sender_id = raw_message.get('user_id')
            target_id = raw_message.get('target_id')
            
            if bot_id and sender_id and target_id:

                # 检查功能是否启用
                if not self._cfg_poke_enabled:
                    return
                
                group_id = event.get_group_id()
                if not group_id:
                    return
                
                group_id_str = str(group_id)
                
                # 检查白名单（如果白名单不为空，则只允许白名单内的群聊）
                if self._cfg_poke_whitelist and group_id_str not in self._cfg_poke_whitelist:
                    return

                # 冷却检查（使用概率替代冷却时间）
                if random.random() > self._cfg_poke_probability:
                    return

                # 检查是否戳的是机器人
                if str(target_id) == str(bot_id):
                    # 随机发送一条语录
                    group_key = str(group_id)
                    effective_group = None if self._cfg_global_mode else group_key
                    q = await self.store.random_one(effective_group)
                    
                    if not q:
                        # 如果没有语录，发送提示
                        yield event.plain_result("本群还没有语录哦，先用「丢出去」保存一条吧~")
                        return
                    
                    # 记录待发送的语录ID（用于删除功能）
                    self._pending_qid[self._session_key(event)] = q.id
                    
                    # 优先发送视频
                    if getattr(q, "videos", None):
                        try:
                            rel = random.choice(q.videos)
                            p = Path(rel)
                            abs_path = p if p.is_absolute() else (self.store.root / rel)
                            if not abs_path.exists() and isinstance(rel, str) and rel.startswith("quotes/"):
                                fixed = rel.split("/", 1)[1] if "/" in rel else rel
                                abs_path = self.store.root / fixed
                            if abs_path.exists():
                                yield event.chain_result([Comp.Video.fromFileSystem(str(abs_path))])
                                return
                        except Exception:
                            pass
                    
                    # 优先发送原图
                    if getattr(q, "images", None):
                        try:
                            rel = random.choice(q.images)
                            p = Path(rel)
                            abs_path = p if p.is_absolute() else (self.store.root / rel)
                            if not abs_path.exists() and isinstance(rel, str) and rel.startswith("quotes/"):
                                fixed = rel.split("/", 1)[1] if "/" in rel else rel
                                abs_path = self.store.root / fixed
                            if abs_path.exists():
                                yield event.chain_result([Comp.Image.fromFileSystem(str(abs_path))])
                                return
                        except Exception:
                            pass
                    
                    # 文本模式
                    if self._cfg_text_mode:
                        yield event.plain_result(f"「{q.text}」 — {q.name}")
                        return
                    
                    # 检查缓存
                    cache_path = self.store.cache_dir / f"{q.id}.png"
                    if self._cfg_render_cache and cache_path.exists():
                        yield event.chain_result([Comp.Image.fromFileSystem(str(cache_path))])
                        return
                    
                    # 渲染语录图
                    output_path = await self._render_quote_image(q)
                    if output_path and output_path.exists():
                        yield event.chain_result([Comp.Image.fromFileSystem(str(output_path))])
                    else:
                        yield event.plain_result(f"「{q.text}」 — {q.name}")
        #endregion



    @filter.command("捡回来")
    async def random_quote(self, event: AstrMessageEvent, uid: str = ""):
        """随机发送一条语录"""
        group_key = str(event.get_group_id() or f"private_{event.get_sender_id()}")
        effective_group = None if self._cfg_global_mode else group_key
        explicit = uid.strip() if (uid and uid.strip().isdigit() and len(uid.strip()) >= 5) else None
        only_qq = explicit or self._extract_at_qq(event)
        q = await (self.store.random_one_by_qq(only_qq, effective_group) if only_qq else self.store.random_one(effective_group))
        if not q:
            if only_qq:
                yield event.plain_result("这个用户还没有语录哦~" if self._cfg_global_mode else "这个用户在本会话还没有语录哦~")
            else:
                yield event.plain_result("还没有语录，先用 丢出去 保存一条吧~" if self._cfg_global_mode else "本会话还没有语录，先用 丢出去 保存一条吧~")
            return
        
        self._pending_qid[self._session_key(event)] = q.id
        
        # 优先发送视频
        if getattr(q, "videos", None):
            try:
                rel = random.choice(q.videos)
                p = Path(rel)
                abs_path = p if p.is_absolute() else (self.store.root / rel)
                if not abs_path.exists() and isinstance(rel, str) and rel.startswith("quotes/"):
                    fixed = rel.split("/", 1)[1] if "/" in rel else rel
                    abs_path = self.store.root / fixed
                if abs_path.exists():
                    yield event.chain_result([Comp.Video.fromFileSystem(str(abs_path))])
                    return
            except Exception as e:
                logger.info(f"随机视频发送失败：{e}")

        # 优先发送原图
        if getattr(q, "images", None):
            try:
                rel = random.choice(q.images)
                p = Path(rel)
                abs_path = p if p.is_absolute() else (self.store.root / rel)
                if not abs_path.exists() and isinstance(rel, str) and rel.startswith("quotes/"):
                    fixed = rel.split("/", 1)[1] if "/" in rel else rel
                    abs_path = self.store.root / fixed
                if abs_path.exists():
                    yield event.chain_result([Comp.Image.fromFileSystem(str(abs_path))])
                    return
            except Exception as e:
                logger.info(f"随机原图发送失败，回退渲染：{e}")
        
        # 文本模式
        if self._cfg_text_mode:
            yield event.plain_result(f"「{q.text}」 — {q.name}")
            return
        
        # 检查缓存
        cache_path = self.store.cache_dir / f"{q.id}.png"
        if self._cfg_render_cache and cache_path.exists():
            yield event.chain_result([Comp.Image.fromFileSystem(str(cache_path))])
            return
        
        # 渲染语录图
        output_path = await self._render_quote_image(q)
        if output_path and output_path.exists():
            yield event.chain_result([Comp.Image.fromFileSystem(str(output_path))])
        else:
            yield event.plain_result(f"「{q.text}」 — {q.name}")

    def _session_key(self, event: AstrMessageEvent) -> str:
        return str(event.get_group_id() or event.unified_msg_origin)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("删除", alias={"删除语录"})
    async def delete_quote(self, event: AstrMessageEvent):
        """删除语录"""
        reply_msg_id: Optional[str] = None
        try:
            for seg in event.get_messages():  # type: ignore[attr-defined]
                if isinstance(seg, Comp.Reply):
                    reply_msg_id = (
                        getattr(seg, "message_id", None)
                        or getattr(seg, "id", None)
                        or getattr(seg, "reply", None)
                        or getattr(seg, "msgId", None)
                    )
                    if reply_msg_id:
                        break
        except Exception as e:
            logger.warning(f"解析 Reply 段失败: {e}")

        if not reply_msg_id:
            yield event.plain_result("请先『回复机器人发送的语录』，再发送 删除。")
            return

        qid: Optional[str] = self._last_sent_qid.get(self._session_key(event))
        if not qid:
            yield event.plain_result("未能定位语录，请先重新发送一次随机语录再尝试删除。")
            return

        ok = await self.store.delete_by_id(qid)
        if ok:
            # 清理渲染缓存
            try:
                cache_path = self.store.cache_dir / f"{qid}.png"
                if cache_path.exists():
                    cache_path.unlink()
            except Exception as e:
                logger.warning(f"清理渲染缓存失败: {e}")
            yield event.plain_result("已删除语录及关联图片。")
        else:
            yield event.plain_result("未找到该语录，可能已被删除。")

    @filter.after_message_sent()
    async def on_after_message_sent(self, event: AstrMessageEvent):
        try:
            key = self._session_key(event)
            qid = self._pending_qid.pop(key, None)
            if qid:
                self._last_sent_qid[key] = qid
        except Exception as e:
            logger.info(f"after_message_sent 记录失败: {e}")

    @filter.command("语录帮助")
    async def help_quote(self, event: AstrMessageEvent):
        """显示语录相关指令的使用说明"""
        help_text = (
            "语录插件帮助\n"
            "- 丢出去：先回复某人的消息，再发送\"丢出去\"（可附带图片）保存为语录。可在消息中 @某人 指定图片语录归属；不@则默认归属上传者。\n"
            "- 捡回来：随机发送一条语录；可用\"捡回来 @某人\"或\"指令前缀+捡回来 12345678\"仅随机该用户的语录；若含用户上传图片，将直接发送原图。\n"
            "- 删除：回复机器人刚发送的随机语录消息，发送\"删除\"或\"删除语录\"进行删除（会同时删除关联图片）。\n"
            "- 戳一戳：在已开启白名单的群聊中戳机器人，有概率随机发送一条语录。\n"
            "- 设置：可在插件设置开启\"全局模式\"以跨群共享语录；关闭则各群/私聊互相隔离。"
        )
        yield event.plain_result(help_text)

    def _get_reply_message_id(self, event: AstrMessageEvent) -> Optional[str]:
        try:
            for seg in event.get_messages():  # type: ignore[attr-defined]
                if isinstance(seg, Comp.Reply):
                    mid = (
                        getattr(seg, "message_id", None)
                        or getattr(seg, "id", None)
                        or getattr(seg, "reply", None)
                        or getattr(seg, "msgId", None)
                    )
                    if mid:
                        return str(mid)
        except Exception as e:
            logger.warning(f"解析 Reply 段失败: {e}")
        return None

    async def _fetch_onebot_msg(self, event: AstrMessageEvent, message_id: str) -> Dict[str, Any]:
        if event.get_platform_name() != "aiocqhttp":
            return {}
        try:
            client = event.bot
            ret = await client.api.call_action("get_msg", message_id=int(str(message_id)))
            return ret or {}
        except Exception as e:
            logger.info(f"get_msg 失败: {e}")
            return {}

    def _extract_at_qq(self, event: AstrMessageEvent) -> Optional[str]:
        try:
            for seg in event.get_messages():  # type: ignore[attr-defined]
                if isinstance(seg, Comp.At):
                    for k in ("qq", "target", "uin", "user_id", "id"):
                        v = getattr(seg, k, None)
                        if v:
                            return str(v)
        except Exception as e:
            logger.warning(f"解析 @ 失败: {e}")
        return None

    async def _resolve_user_name(self, event: AstrMessageEvent, qq: str) -> str:
        if event.get_platform_name() == "aiocqhttp":
            try:
                group_id = event.get_group_id()
                client = event.bot
                if group_id:
                    payloads = {"group_id": int(group_id), "user_id": int(qq), "no_cache": True}
                    ret = await client.api.call_action("get_group_member_info", **payloads)
                    card = (ret.get("card") or "").strip()
                    nickname = (ret.get("nickname") or "").strip()
                    if card or nickname:
                        return card or nickname
                else:
                    payloads = {"user_id": int(qq), "no_cache": True}
                    ret = await client.api.call_action("get_stranger_info", **payloads)
                    nickname = (ret.get("nickname") or "").strip()
                    if nickname:
                        return nickname
            except Exception as e:
                logger.info(f"读取 Napcat 用户信息失败，回退：{e}")
        return str(qq)

    async def _avatar_url(self, qq: str) -> str:
        """获取 QQ 头像的 base64 data URI，避免跨域问题"""
        avatar_dir = self.store.root / "avatars"
        avatar_dir.mkdir(parents=True, exist_ok=True)
        local_avatar = avatar_dir / f"{qq}.jpg"
        
        # 如果本地已缓存，读取并转换为 base64
        if local_avatar.exists():
            try:
                import base64
                img_data = local_avatar.read_bytes()
                b64 = base64.b64encode(img_data).decode('utf-8')
                return f"data:image/jpeg;base64,{b64}"
            except Exception as e:
                logger.warning(f"读取本地头像失败: {e}")
        
        # 下载头像并保存
        try:
            url = f"https://q1.qlogo.cn/g?b=qq&nk={qq}&s=640"
            if self.http_client:
                resp = await self.http_client.get(url)
                content = resp.content
            else:
                import httpx
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(url)
                    content = resp.content
            
            # 保存到本地
            local_avatar.write_bytes(content)
            
            # 转换为 base64 返回
            import base64
            b64 = base64.b64encode(content).decode('utf-8')
            return f"data:image/jpeg;base64,{b64}"
        except Exception as e:
            logger.warning(f"下载头像失败: {e}，返回默认占位符")
            # 返回一个 1x1 透明 PNG 作为占位符
            return "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="

    def _extract_plaintext_from_onebot_message(self, message) -> Optional[str]:
        try:
            if isinstance(message, list):
                parts: List[str] = []
                for m in message:
                    t = m.get("type")
                    d = m.get("data") or {}
                    if t in ("text", "plain"):
                        parts.append(str(d.get("text") or ""))
                text = "".join(parts).strip()
                return text if text else None
        except Exception:
            pass
        return None

    async def _ingest_images_from_onebot_message(self, event: AstrMessageEvent, message, group_key: str) -> List[str]:
        saved: List[str] = []
        try:
            if not isinstance(message, list):
                return saved
            for m in message:
                try:
                    if (m.get("type") or "").lower() != "image":
                        continue
                    d = m.get("data") or {}
                    url = d.get("url") or d.get("image_url")
                    if url and (str(url).startswith("http://") or str(url).startswith("https://")):
                        rel = await self.store.save_image_from_url(str(url), group_key)
                        if rel:
                            saved.append(rel)
                            continue
                    file_id_or_path = d.get("file") or d.get("path")
                    if file_id_or_path:
                        try:
                            client = event.bot
                            ret = await client.api.call_action("get_image", file=str(file_id_or_path))
                            local_path = ret.get("file") or ret.get("path") or ret.get("file_path")
                            if local_path:
                                rel = await self.store.save_image_from_fs(str(local_path), group_key)
                                if rel:
                                    saved.append(rel)
                                    continue
                        except Exception as e:
                            logger.info(f"get_image 回退失败: {e}")
                        rel = await self.store.save_image_from_fs(str(file_id_or_path), group_key)
                        if rel:
                            saved.append(rel)
                except Exception as e:
                    logger.warning(f"处理图片段失败: {e}")
        except Exception as e:
            logger.warning(f"解析 OneBot 消息图片失败: {e}")
        return list(dict.fromkeys(saved))

    async def _ingest_videos_from_onebot_message(self, event: AstrMessageEvent, message, group_key: str) -> List[str]:
        saved: List[str] = []
        try:
            if not isinstance(message, list):
                return saved
            for m in message:
                try:
                    if (m.get("type") or "").lower() != "video":
                        continue
                    d = m.get("data") or {}
                    url = d.get("url") or d.get("video_url")
                    if url and (str(url).startswith("http://") or str(url).startswith("https://")):
                        rel = await self.store.save_video_from_url(str(url), group_key)
                        if rel:
                            saved.append(rel)
                            continue
                    
                    file_id_or_path = d.get("file") or d.get("path") or d.get("file_id")
                    if file_id_or_path:
                        # 尝试通过 get_file API 获取下载链接 (针对 WS 协议或异地部署)
                        if event.get_platform_name() == "aiocqhttp":
                            try:
                                client = event.bot
                                # 尝试调用 get_file (NapCat/Go-CQHTTP 等扩展 API)
                                ret = await client.api.call_action("get_file", file_id=str(file_id_or_path))
                                dl_url = ret.get("url") or ret.get("file_url") or ret.get("download_url")
                                if dl_url and (str(dl_url).startswith("http") or str(dl_url).startswith("https")):
                                    rel = await self.store.save_video_from_url(str(dl_url), group_key)
                                    if rel:
                                        saved.append(rel)
                                        continue
                            except Exception as e:
                                logger.debug(f"尝试 get_file 获取视频链接失败 (通常因为协议端不支持或文件未上传): {e}")

                        # 视频通常没有 get_video API，直接尝试保存本地路径
                        rel = await self.store.save_video_from_fs(str(file_id_or_path), group_key)
                        if rel:
                            saved.append(rel)
                except Exception as e:
                    logger.warning(f"处理视频段失败: {e}")
        except Exception as e:
            logger.warning(f"解析 OneBot 消息视频失败: {e}")
        return list(dict.fromkeys(saved))

    async def _ingest_images_from_segments(self, event: AstrMessageEvent, group_key: str) -> List[str]:
        saved: List[str] = []
        try:
            for seg in event.get_messages():  # type: ignore[attr-defined]
                if isinstance(seg, Comp.Image):
                    url = getattr(seg, "url", None)
                    file_or_path = getattr(seg, "file", None) or getattr(seg, "path", None)
                    if url and (str(url).startswith("http://") or str(url).startswith("https://")):
                        rel = await self.store.save_image_from_url(str(url), group_key)
                        if rel:
                            saved.append(rel)
                    elif file_or_path:
                        rel = await self.store.save_image_from_fs(str(file_or_path), group_key)
                        if rel:
                            saved.append(rel)
                        elif event.get_platform_name() == "aiocqhttp":
                            try:
                                client = event.bot
                                ret = await client.api.call_action("get_image", file=str(file_or_path))
                                local_path = ret.get("file") or ret.get("path") or ret.get("file_path")
                                if local_path:
                                    rel = await self.store.save_image_from_fs(str(local_path), group_key)
                                    if rel:
                                        saved.append(rel)
                            except Exception as e:
                                logger.info(f"segments get_image 回退失败: {e}")
        except Exception as e:
            logger.warning(f"解析 Comp.Image 失败: {e}")
        return list(dict.fromkeys(saved))

    async def _ingest_videos_from_segments(self, event: AstrMessageEvent, group_key: str) -> List[str]:
        saved: List[str] = []
        try:
            for seg in event.get_messages():  # type: ignore[attr-defined]
                if isinstance(seg, Comp.Video):
                    url = getattr(seg, "url", None)
                    file_or_path = getattr(seg, "file", None) or getattr(seg, "path", None)
                    if url and (str(url).startswith("http://") or str(url).startswith("https://")):
                        rel = await self.store.save_video_from_url(str(url), group_key)
                        if rel:
                            saved.append(rel)
                    elif file_or_path:
                        # 尝试通过 get_file API 获取下载链接
                        if event.get_platform_name() == "aiocqhttp":
                            try:
                                client = event.bot
                                ret = await client.api.call_action("get_file", file_id=str(file_or_path))
                                dl_url = ret.get("url") or ret.get("file_url") or ret.get("download_url")
                                if dl_url and (str(dl_url).startswith("http") or str(dl_url).startswith("https")):
                                    rel = await self.store.save_video_from_url(str(dl_url), group_key)
                                    if rel:
                                        saved.append(rel)
                                        continue
                            except Exception as e:
                                pass

                        rel = await self.store.save_video_from_fs(str(file_or_path), group_key)
                        if rel:
                            saved.append(rel)
        except Exception as e:
            logger.warning(f"解析 Comp.Video 失败: {e}")
        return list(dict.fromkeys(saved))

    async def _render_quote_image(self, q: Quote) -> Optional[Path]:
        """使用 Playwright 渲染语录图片，返回本地文件路径"""
        width = int(self.img_cfg.get("width", 1280))
        # 移除固定高度，改为基于内容计算
        min_height = int(self.img_cfg.get("min_height", 300))
        bg_color = self.img_cfg.get("bg_color", "#1a1a2e")
        text_color = self.img_cfg.get("text_color", "#eaeaea")
        font_family = self.img_cfg.get("font_family", "'Noto Serif SC'")

        avatar = await self._avatar_url(q.qq)
        safe_text = self._strip_at_tokens(q.text)
        # escaped_text = html.escape(safe_text) # Jinja2 默认会转义
        
        # 解析 group 字段获取来源信息
        group_display = "私聊"
        if q.group and not q.group.startswith("private_"):
            group_display = f"群 {q.group}"
        elif q.group and q.group.startswith("private_"):
            group_display = "私聊"

        # 使用 Jinja2 渲染模板
        try:
            template = self.jinja_env.get_template("quote_card.html")
            html_content = template.render(
                width=width,
                min_height=min_height,
                bg_color=bg_color,
                text_color=text_color,
                font_family=font_family,
                avatar_url=avatar,
                name=q.name,
                text=safe_text,
                group_info=group_display,
                qq_num=q.qq
            )
        except Exception as e:
            logger.error(f"Jinja2 模板渲染失败: {e}")
            return None

        # 使用 Playwright 渲染
        if self._browser:
            try:
                from time import time
                
                # 确保目录存在
                self.store.cache_dir.mkdir(parents=True, exist_ok=True)
                
                # 临时 HTML 文件
                temp_html = self.store.cache_dir / f"temp_{int(time()*1000)}_{random.randint(1000,9999)}.html"
                temp_html.write_text(html_content, encoding='utf-8')
                
                # 输出图片路径
                output_path = self.store.cache_dir / f"{q.id}.png"
                
                context = await self._browser.new_context(
                    viewport={"width": width, "height": min_height},  # 初始使用最小高度
                    device_scale_factor=2
                )
                page = await context.new_page()
                
                await page.goto(f"file:///{temp_html.resolve().as_posix()}")
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(1000)  # 给JS调整高度的时间
                
                # 获取实际内容高度
                actual_height = await page.evaluate("""
                    () => {
                        const container = document.querySelector('.game-container');
                        return container ? Math.max(container.scrollHeight, container.clientHeight) : document.documentElement.scrollHeight;
                    }
                """)
                actual_height = int(actual_height) - 20
                
                # 重新设置视口高度为实际内容高度
                await page.set_viewport_size({"width": width, "height": actual_height})
                
                await page.screenshot(
                    path=str(output_path),
                    type="png",
                    clip={"x": 0, "y": 0, "width": width, "height": actual_height},
                    full_page=False
                )
                
                await page.close()
                await context.close()
                
                # 清理临时文件
                try:
                    temp_html.unlink()
                except Exception:
                    pass
                
                return output_path
                
            except Exception as e:
                logger.error(f"Playwright 渲染失败: {e}")
                return None
        else:
            logger.warning("Playwright 不可用，无法渲染图片")
            return None

    async def terminate(self):
        """插件卸载/停用时回调"""
        try:
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
        except Exception as e:
            logger.warning(f"关闭 Playwright 失败: {e}")
        try:
            if getattr(self, "http_client", None):
                await self.http_client.aclose()
        except Exception:
            pass

    async def _clean_expired_cache(self):
        """清理过期的渲染缓存图片"""
        try:
            import time
            if not self.store.cache_dir.exists():
                return
            
            now = time.time()
            retention_sec = self._cfg_cache_retention * 24 * 3600
            count = 0
            
            for file_path in self.store.cache_dir.iterdir():
                if file_path.is_file() and file_path.suffix.lower() == ".png":
                    try:
                        mtime = file_path.stat().st_mtime
                        if now - mtime > retention_sec:
                            file_path.unlink()
                            count += 1
                    except Exception as e:
                        logger.debug(f"清理缓存文件失败 {file_path}: {e}")
            
            if count > 0:
                logger.info(f"已清理 {count} 个过期的语录渲染缓存文件 (保留 {self._cfg_cache_retention} 天)")
                
        except Exception as e:
            logger.warning(f"清理过期缓存失败: {e}")

    @filter.command("丢出去")
    async def add_quote_alias(self, event: AstrMessageEvent, uid: str = ""):
        async for res in self.add_quote(event, uid=uid):
            yield res

    def _strip_at_tokens(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"@[^@\s（）()]+(?:[（(]\d{5,}[）)])?", "", text)
        text = text.replace("@全体成员", "")
        return " ".join(text.split())