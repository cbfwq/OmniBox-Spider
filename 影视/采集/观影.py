# -*- coding: utf-8 -*-
# //@name:观影爬虫
# //@id:guanying_adaptive
# //@version:28

import ast
import base64
import html as html_lib
import inspect
import json
import re
import threading
import time
from collections import OrderedDict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit

import requests
from lxml import html

from base.spider import Spider as BaseSpider


class Spider(BaseSpider):
    RUNTIME_TAG = "GUANYING-V28"
    ADDRESS_PAGE = "https://www.xn--ykq321c.com/"
    BOOTSTRAP_HOST = "https://www.xn--10vr61a3xc5x3b.com"
    FALLBACK_HOSTS = (
        "https://www.xn--74qz10cqsltibh40akss.com",
        "https://www.xn--10vr61a3xc5x3b.com",
        "https://www.xn--vcsx1ip8b8w4i.com",
        "https://www.xn--wcv59z.com",
        "https://www.xn--kivn76b41nnhi.com",
        "https://www.hgeme.com",
    )
    CATEGORIES = (
        ("mv", "电影"),
        ("tv", "剧集"),
        ("ac", "动漫"),
        ("hits/mv", "热门电影"),
        ("hits/tv", "热门剧集"),
        ("hits/ac", "热门动漫"),
    )
    CATEGORY_NAMES = dict(CATEGORIES)
    PROVIDER_ORDER = (
        "115网盘",
        "夸克网盘",
        "阿里网盘",
        "百度网盘",
        "迅雷网盘",
        "UC网盘",
        "天翼网盘",
        "123网盘",
        "移动网盘",
        "微云",
        "其他网盘",
    )
    PROVIDER_TYPE_NAMES = {
        0: "迅雷网盘",
        1: "百度网盘",
        2: "夸克网盘",
        3: "天翼网盘",
        4: "UC网盘",
        5: "阿里网盘",
        6: "115网盘",
        7: "123网盘",
        8: "移动网盘",
        9: "微云",
    }
    PROVIDER_DOMAIN_NAMES = (
        ("115.com", "115网盘"),
        ("anxia.com", "115网盘"),
        ("pan.quark.cn", "夸克网盘"),
        ("alipan.com", "阿里网盘"),
        ("aliyundrive.com", "阿里网盘"),
        ("pan.baidu.com", "百度网盘"),
        ("pan.xunlei.com", "迅雷网盘"),
        ("drive.uc.cn", "UC网盘"),
        ("cloud.189.cn", "天翼网盘"),
        ("123pan.com", "123网盘"),
        ("caiyun.139.com", "移动网盘"),
        ("share.weiyun.com", "微云"),
    )
    PROVIDER_ACCEL_KEYS = {
        "115网盘": "PAN115",
        "夸克网盘": "QUARK",
        "阿里网盘": "ALI",
        "百度网盘": "BAIDU",
        "迅雷网盘": "XUNLEI",
        "UC网盘": "UC",
        "天翼网盘": "PAN189",
        "123网盘": "PAN123",
        "移动网盘": "PAN139",
        "微云": "WEIYUN",
        "光雅网盘": "GUANGYA",
    }
    MAGNET_RE = re.compile(
        r"magnet:\?xt=urn:btih:([A-Fa-f0-9]{40}|[A-Z2-7]{32})(?:&[^\s\"'<>]*)?",
        re.I,
    )
    BTIH_RE = re.compile(r"btih:([A-Fa-f0-9]{40}|[A-Z2-7]{32})", re.I)
    SUBTITLE_RE = re.compile(r"中文字幕|简繁|简中|繁中|中字|字幕|CHS|CHT|SUB", re.I)
    QUALITY_4K_RE = re.compile(r"2160|4K|UHD|HDR|Dolby\s*Vision|杜比视界|\bDV\b", re.I)
    QUALITY_1080_RE = re.compile(r"1080|FHD|BluRay|WEB[- .]?DL", re.I)
    NEGATIVE_RE = re.compile(r"广告|推广|sample|预告|抢先|TC|TS|CAM|枪版", re.I)
    VIDEO_EXT_RE = re.compile(r"\.(mkv|mp4|avi|mov|wmv|flv|ts|m2ts|webm)(?:$|[?#])", re.I)
    STATUS_PREFIX = "guale-status:"
    PRIVATE_PREFIX = "guale-v1:"
    PACK_KEYS = {
        "kind": "k",
        "magnet": "m",
        "magnets": "g",
        "url": "u",
        "urls": "x",
        "pwd": "p",
        "provider": "r",
        "profile": "q",
        "profiles": "o",
        "dir": "d",
        "id": "i",
        "title": "t",
        "source": "s",
        "episode": "e",
        "label": "l",
    }
    IMAGE_HOST = "https://s.tutu.pm"
    MAX_CACHE = 48

    def __init__(self):
        self.name = "观影爬虫"
        self.address_page = self.ADDRESS_PAGE
        self.username = ""
        self.password = ""
        self.timeout = 15
        self.probe_timeout = 5
        self.probe_ttl = 300
        self.probe_each_request = False
        self.max_magnets = 40
        self.magnet_fallback_links = 2
        self.magnet_raw_fallback = True
        self.max_per_pan = 40
        self.max_online_groups = 24
        self.online_probe_workers = 3
        self.online_probe_deadline_ms = 1800
        self.online_quality_grace_ms = 180
        self.online_probe_bytes = 4096
        self.online_result_ttl = 90
        self.search_cache_ttl = 300
        self.search_stale_ttl = 1800
        self.search_quality_grace_ms = 120
        self.category_cache_ttl = 300
        self.category_stale_ttl = 1800
        self.max_probe_hosts = 4
        self.category_wait_timeout = 8
        self.host_fail_cooldown = 120
        self.enable_hedged_requests = True
        self.hedge_delay_ms = 250
        self.warm_standby_mirror = True
        self.direct_only = True
        self.mirror_config_url = ""
        self.address_pages = [self.address_page]
        self.fallback_hosts = list(self.FALLBACK_HOSTS)
        self.enable_online_sources = True
        self.show_empty_magnet_group = True
        self.expand_pan_playlists = True
        # Cached candidates render immediately; uncached providers expand in
        # background so detail pages do not wait on remote cloud-drive APIs.
        # V28 enables the larger budget after init(); direct unit-test
        # construction keeps the historical V25 budget until then.
        self.aggressive_detail_prewarm = False
        self.pan_expand_providers = 0
        self.pan_expand_attempts = 2
        self.pan_fallback_links = 3
        self.pan_expand_workers = 4
        self.pan_background_workers = 4
        self.pan_background_job_limit = 8
        self.backend_play_cache_ttl = 20
        self.pan_click_inflight_wait = 8
        self.pan_failure_cooldown = 300
        self.pan_link_failure_cooldown = 1800
        self.pan_capability_failure_cooldown = 1800
        self.pan_rate_limit_cooldown = 120
        self.pan_transient_failure_cooldown = 20
        self.pan_unknown_failure_cooldown = 60
        self.metrics_log_interval = 60
        self.alist_api = ""
        self.alist_token = ""
        self.backend_parse = False
        self.category_mode = False
        self.categoryMode = False
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
        self._lock = threading.RLock()
        self._cache_lock = threading.Lock()
        self._sessions = {}
        self._session_times = {}
        self._failed_hosts = set()
        self._failed_until = {}
        self._host = ""
        self._host_checked_at = 0.0
        self._last_candidates = []
        self._candidate_cache = []
        self._candidate_cached_at = 0.0
        self._last_diag = {}
        self._page_cache = OrderedDict()
        self._resource_cache = OrderedDict()
        self._online_history = OrderedDict()
        self._online_result_cache = OrderedDict()
        self._search_cache = OrderedDict()
        self._search_inflight = {}
        self._refresh_inflight = set()
        self._pan_link_history = OrderedDict()
        self._backend_parse_cache = OrderedDict()
        self._backend_play_cache = OrderedDict()
        self._backend_session_lock = threading.Lock()
        self._backend_sessions = {}
        self._backend_parse_failures = {}
        self._backend_parse_failure_errors = {}
        self._backend_parse_inflight = {}
        self._pan_capability = {}
        self._pan_capability_configured = False
        self._warmup_guard = threading.Lock()
        self._warmup_event = threading.Event()
        self._warmup_started = False
        self._warmup_generation = 0
        self._standby_generation = -1
        self._pan_background_lock = threading.Lock()
        self._pan_background_urls = set()
        self._pan_generation = 0
        self._online_executor_lock = threading.Lock()
        self._online_executor = None
        self._search_executor_lock = threading.Lock()
        self._search_inflight_lock = threading.Lock()
        self._search_executor = None
        self._refresh_executor_lock = threading.Lock()
        self._refresh_executor = None
        self._hedge_executor_lock = threading.Lock()
        self._hedge_executor = None
        self._standby_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._metrics = {}
        self._metric_latencies = {}
        self._metrics_last_log = time.time()
        self._pan_parse_stats = {
            "attempts": 0,
            "successes": 0,
            "ewma_ms": 0.0,
        }

    def getName(self):
        return self.name

    def init(self, extend=""):
        config = self._config(extend)
        runtime = self._atvp_runtime_context()
        if runtime.get("local_proxy_config"):
            config = dict(config)
            config["local_proxy_config"] = runtime["local_proxy_config"]
        self.address_page = self._http_url_value(config.get("address_page"), self.address_page)
        configured_pages = config.get("address_pages") or config.get("publish_pages") or []
        if isinstance(configured_pages, str):
            configured_pages = re.split(r"[\s,;]+", configured_pages)
        self.address_pages = self._http_url_list([self.address_page] + list(configured_pages or []))
        self.mirror_config_url = self._http_url_value(
            config.get("mirror_config_url") or config.get("host_config_url"),
            self.mirror_config_url,
        )
        configured_hosts = config.get("fallback_hosts") or config.get("mirror_hosts") or []
        if isinstance(configured_hosts, str):
            configured_hosts = re.split(r"[\s,;]+", configured_hosts)
        self.fallback_hosts = self._url_list(list(self.FALLBACK_HOSTS) + list(configured_hosts or []))
        self.username = str(
            config.get("site_username") or config.get("username") or self.username
        ).strip()
        self.password = str(
            config.get("site_password") or config.get("password") or self.password
        )
        self.alist_api = self._url_value(runtime.get("api"), "")
        self.alist_token = str(runtime.get("token") or "").strip()
        with self._pan_background_lock:
            self._pan_generation += 1
            self._pan_background_urls.clear()
        self._configure_pan_capabilities(config)
        self.probe_each_request = self._bool_value(
            config.get("probe_each_request"), self.probe_each_request
        )
        self.max_probe_hosts = self._bounded_int(
            config.get("max_probe_hosts"), self.max_probe_hosts, 2, 20
        )
        self.host_fail_cooldown = self._bounded_int(
            config.get("host_fail_cooldown"), self.host_fail_cooldown, 15, 1800
        )
        self.enable_hedged_requests = self._bool_value(
            config.get("enable_hedged_requests"), self.enable_hedged_requests
        )
        self.hedge_delay_ms = self._bounded_int(
            config.get("hedge_delay_ms"), self.hedge_delay_ms, 100, 1000
        )
        self.warm_standby_mirror = self._bool_value(
            config.get("warm_standby_mirror"), self.warm_standby_mirror
        )
        self.direct_only = self._bool_value(
            config.get("direct_only"), self.direct_only
        )
        self.timeout = self._bounded_int(config.get("timeout"), self.timeout, 8, 45)
        self.probe_timeout = self._bounded_int(
            config.get("probe_timeout"), self.probe_timeout, 5, 20
        )
        self.probe_ttl = self._bounded_int(config.get("probe_ttl"), self.probe_ttl, 0, 1800)
        self.max_magnets = self._bounded_int(
            config.get("max_magnets"), self.max_magnets, 5, 100
        )
        self.magnet_fallback_links = self._bounded_int(
            config.get("magnet_fallback_links"), self.magnet_fallback_links, 1, 3
        )
        self.magnet_raw_fallback = self._bool_value(
            config.get("magnet_raw_fallback"), self.magnet_raw_fallback
        )
        self.max_per_pan = self._bounded_int(
            config.get("max_per_pan"), self.max_per_pan, 5, 150
        )
        self.max_online_groups = self._bounded_int(
            config.get("max_online_groups"), self.max_online_groups, 1, 50
        )
        self.online_probe_workers = self._bounded_int(
            config.get("online_probe_workers"), self.online_probe_workers, 2, 4
        )
        self.online_probe_deadline_ms = self._bounded_int(
            config.get("online_probe_deadline_ms"), self.online_probe_deadline_ms, 600, 4000
        )
        self.online_quality_grace_ms = self._bounded_int(
            config.get("online_quality_grace_ms"), self.online_quality_grace_ms, 0, 800
        )
        self.online_probe_bytes = self._bounded_int(
            config.get("online_probe_bytes"), self.online_probe_bytes, 512, 8192
        )
        self.online_result_ttl = self._bounded_int(
            config.get("online_result_ttl"), self.online_result_ttl, 15, 300
        )
        self.search_cache_ttl = self._bounded_int(
            config.get("search_cache_ttl"), self.search_cache_ttl, 30, 1800
        )
        self.search_stale_ttl = self._bounded_int(
            config.get("search_stale_ttl"), self.search_stale_ttl,
            self.search_cache_ttl, 7200
        )
        self.search_quality_grace_ms = self._bounded_int(
            config.get("search_quality_grace_ms"), self.search_quality_grace_ms, 0, 500
        )
        self.category_cache_ttl = self._bounded_int(
            config.get("category_cache_ttl"), self.category_cache_ttl, 30, 1800
        )
        self.category_stale_ttl = self._bounded_int(
            config.get("category_stale_ttl"), self.category_stale_ttl,
            self.category_cache_ttl, 7200
        )
        self.enable_online_sources = self._bool_value(
            config.get("enable_online_sources"), self.enable_online_sources
        )
        self.show_empty_magnet_group = self._bool_value(
            config.get("show_empty_magnet_group"), self.show_empty_magnet_group
        )
        self.expand_pan_playlists = self._bool_value(
            config.get("expand_pan_playlists"), self.expand_pan_playlists
        )
        self.aggressive_detail_prewarm = self._bool_value(
            config.get("aggressive_detail_prewarm"), True
        )
        self.pan_expand_providers = self._bounded_int(
            config.get("pan_expand_providers"), self.pan_expand_providers, 0, 12
        )
        self.pan_expand_attempts = self._bounded_int(
            config.get("pan_expand_attempts"),
            3 if self.aggressive_detail_prewarm else self.pan_expand_attempts,
            1,
            30,
        )
        self.pan_fallback_links = self._bounded_int(
            config.get("pan_fallback_links"), self.pan_fallback_links, 0, 12
        )
        self.pan_expand_workers = self._bounded_int(
            config.get("pan_expand_workers"), self.pan_expand_workers, 1, 8
        )
        self.pan_background_workers = self._bounded_int(
            config.get("pan_background_workers"),
            6 if self.aggressive_detail_prewarm else self.pan_background_workers,
            1,
            8,
        )
        self.pan_background_job_limit = self._bounded_int(
            config.get("pan_background_job_limit"),
            12 if self.aggressive_detail_prewarm else self.pan_background_job_limit,
            2,
            24,
        )
        self.backend_play_cache_ttl = self._bounded_int(
            config.get("backend_play_cache_ttl"), self.backend_play_cache_ttl, 5, 120
        )
        self.pan_click_inflight_wait = self._bounded_int(
            config.get("pan_click_inflight_wait"), self.pan_click_inflight_wait, 2, 20
        )
        self.pan_failure_cooldown = self._bounded_int(
            config.get("pan_failure_cooldown"), self.pan_failure_cooldown, 30, 1800
        )
        self.pan_link_failure_cooldown = self._bounded_int(
            config.get("pan_link_failure_cooldown"), self.pan_link_failure_cooldown, 60, 7200
        )
        self.pan_capability_failure_cooldown = self._bounded_int(
            config.get("pan_capability_failure_cooldown"),
            self.pan_capability_failure_cooldown, 300, 7200
        )
        self.pan_rate_limit_cooldown = self._bounded_int(
            config.get("pan_rate_limit_cooldown"), self.pan_rate_limit_cooldown, 30, 1800
        )
        self.pan_transient_failure_cooldown = self._bounded_int(
            config.get("pan_transient_failure_cooldown"),
            self.pan_transient_failure_cooldown, 5, 300
        )
        self.pan_unknown_failure_cooldown = self._bounded_int(
            config.get("pan_unknown_failure_cooldown"),
            self.pan_unknown_failure_cooldown, 15, 600
        )
        self.metrics_log_interval = self._bounded_int(
            config.get("metrics_log_interval"), self.metrics_log_interval, 30, 600
        )
        self.category_wait_timeout = self._bounded_int(
            config.get("category_wait_timeout"), self.category_wait_timeout, 1, 12
        )
        with self._lock:
            self._sessions.clear()
            self._session_times.clear()
            self._failed_hosts.clear()
            self._failed_until.clear()
            self._host = self._normalize_host(
                config.get("bootstrap_host") or self.BOOTSTRAP_HOST
            )
            self._host_checked_at = time.time() if self._host else 0.0
            self._candidate_cache = []
            self._candidate_cached_at = 0.0
        self._close_backend_sessions()
        with self._cache_lock:
            self._page_cache.clear()
            self._resource_cache.clear()
            self._online_history.clear()
            self._online_result_cache.clear()
            self._search_cache.clear()
            self._pan_link_history.clear()
            self._backend_parse_cache.clear()
            self._backend_play_cache.clear()
            self._backend_parse_failures.clear()
            self._backend_parse_failure_errors.clear()
            for event in self._backend_parse_inflight.values():
                event.set()
            self._backend_parse_inflight.clear()
            self._pan_capability = dict(self._pan_capability)
        with self._search_inflight_lock:
            self._search_inflight.clear()
            self._refresh_inflight.clear()
        with self._warmup_guard:
            self._warmup_generation += 1
            self._warmup_event = threading.Event()
            self._warmup_started = False
        with self._online_executor_lock:
            executor = self._online_executor
            self._online_executor = None
        if executor is not None:
            executor.shutdown(wait=False)
        with self._search_executor_lock:
            search_executor = self._search_executor
            self._search_executor = None
        if search_executor is not None:
            search_executor.shutdown(wait=False)
        with self._refresh_executor_lock:
            refresh_executor = self._refresh_executor
            self._refresh_executor = None
        if refresh_executor is not None:
            refresh_executor.shutdown(wait=False)
        with self._hedge_executor_lock:
            hedge_executor = self._hedge_executor
            self._hedge_executor = None
        if hedge_executor is not None:
            hedge_executor.shutdown(wait=False)
        with self._metrics_lock:
            self._metrics.clear()
            self._metric_latencies.clear()
            self._metrics_last_log = time.time()
            self._pan_parse_stats = {"attempts": 0, "successes": 0, "ewma_ms": 0.0}
        self._log(
            "init",
            direct_only=int(self.direct_only),
            bootstrap=self._host or "none",
            timeout=self.timeout,
            probe_timeout=self.probe_timeout,
        )
        # Preheat mirror selection, authentication and the first page without
        # delaying plugin initialization or the caller's first UI frame.
        self._start_warmup()

    def destroy(self):
        with self._warmup_guard:
            self._warmup_generation += 1
            self._warmup_event.set()
        self._close_backend_sessions()
        with self._online_executor_lock:
            executor = self._online_executor
            self._online_executor = None
        if executor is not None:
            executor.shutdown(wait=False)
        with self._search_executor_lock:
            search_executor = self._search_executor
            self._search_executor = None
        if search_executor is not None:
            search_executor.shutdown(wait=False)
        with self._refresh_executor_lock:
            refresh_executor = self._refresh_executor
            self._refresh_executor = None
        if refresh_executor is not None:
            refresh_executor.shutdown(wait=False)
        with self._hedge_executor_lock:
            hedge_executor = self._hedge_executor
            self._hedge_executor = None
        if hedge_executor is not None:
            hedge_executor.shutdown(wait=False)
        with self._cache_lock:
            for event in self._backend_parse_inflight.values():
                event.set()
            self._backend_parse_inflight.clear()
        with self._lock:
            for session in self._sessions.values():
                try:
                    session.close()
                except Exception:
                    pass
            self._sessions.clear()

    def isVideoFormat(self, url):
        return bool(self.VIDEO_EXT_RE.search(str(url or "")))

    def manualVideoCheck(self):
        return False

    def homeContent(self, filter):
        self._log("home", action="start")
        self._start_warmup()
        return {
            "class": [
                {"type_id": type_id, "type_name": type_name}
                for type_id, type_name in self.CATEGORIES
            ],
            "filters": {},
        }

    def homeVideoContent(self):
        cache_key = ("mv", 1)
        cache_state, cached, age = self._page_cache_state(cache_key)
        if cache_state == "fresh":
            self._record_metric("category_cache_hit")
            self._log("home-video", state="cache", count=len(cached.get("list", [])))
            return {"list": cached.get("list", [])}
        if cache_state == "stale":
            self._record_metric("category_stale_hit")
            self._schedule_category_refresh(cache_key, "mv", 1)
            self._log("home-video", state="stale", age=int(age))
            return {"list": cached.get("list", [])}
        self._start_warmup()
        if not self._warmup_event.is_set():
            self._warmup_event.wait(self.category_wait_timeout)
        cached = self._cached_page(("mv", 1))
        if cached is not None:
            self._log("home-video", state="warm-cache", count=len(cached.get("list", [])))
            return {"list": cached.get("list", [])}
        self._log("home-video", state="foreground")
        try:
            page = self._fetch_category_page("mv", 1)
            return {"list": page.get("list", [])}
        except Exception as exc:
            self._diag("home-video", error=str(exc))
            self._log("home-video", state="error", error=type(exc).__name__)
            return {"list": []}

    def categoryContent(self, tid, pg, filter, extend):
        started = time.perf_counter()
        page = self._page_number(pg)
        category = str(tid or "mv").strip().strip("/")
        if category not in self.CATEGORY_NAMES:
            category = "mv"
        cache_key = (category, page)
        cache_state, cached, age = self._page_cache_state(cache_key)
        if cache_state == "fresh":
            self._record_metric("category_cache_hit")
            self._log(
                "category",
                state="cache",
                tid=category,
                page=page,
                count=len(cached.get("list", [])),
            )
            return cached
        if cache_state == "stale":
            self._record_metric("category_stale_hit")
            self._schedule_category_refresh(cache_key, category, page)
            self._log(
                "category",
                state="stale",
                tid=category,
                page=page,
                age=int(age),
                count=len(cached.get("list", [])),
            )
            return cached
        self._log("category", state="start", tid=category, page=page)
        self._start_warmup()
        if not self._warmup_event.is_set():
            self._warmup_event.wait(self.category_wait_timeout)
            cached = self._cached_page(cache_key)
            if cached is not None:
                self._log(
                    "category",
                    state="warm-cache",
                    tid=category,
                    page=page,
                    count=len(cached.get("list", [])),
                    elapsed_ms=int((time.perf_counter() - started) * 1000),
                )
                return cached
            if not self._warmup_event.is_set():
                self._log(
                    "category",
                    state="foreground",
                    tid=category,
                    page=page,
                    elapsed_ms=int((time.perf_counter() - started) * 1000),
                )
        try:
            parsed = self._fetch_category_page(category, page)
            self._log(
                "category",
                state="ok",
                tid=category,
                page=page,
                count=len(parsed.get("list", [])),
                elapsed_ms=int((time.perf_counter() - started) * 1000),
            )
            return parsed
        except Exception as exc:
            self._diag("category", error=str(exc), category=category, page=page)
            self._log(
                "category",
                state="error",
                tid=category,
                page=page,
                error=type(exc).__name__,
                elapsed_ms=int((time.perf_counter() - started) * 1000),
            )
            return self._empty_page(page, "分类读取失败: %s" % exc)

    def searchContent(self, key, quick, pg="1"):
        page = self._page_number(pg)
        keyword = self._clean_text(key)
        if not keyword:
            return self._empty_page(page)
        cache_key = (keyword.lower(), page)
        cache_state, cached, age = self._search_cache_state(cache_key)
        if cache_state == "fresh":
            self._record_metric("search_cache_hit")
            self._log("search", state="cache", keyword=keyword, page=page)
            return cached
        if cache_state == "stale":
            self._record_metric("search_stale_hit")
            self._schedule_search_refresh(cache_key, keyword, page)
            self._log(
                "search", state="stale", keyword=keyword, page=page, age=int(age)
            )
            return cached
        started = time.perf_counter()
        try:
            futures = self._search_futures(cache_key, keyword, page)
            result = self._select_search_result(futures, page)
            if result.get("list"):
                self._remember_search(cache_key, result, 1)
            self._log(
                "search",
                state="ok" if result.get("list") else "empty",
                keyword=keyword,
                page=page,
                count=len(result.get("list", [])),
                elapsed_ms=int((time.perf_counter() - started) * 1000),
            )
            return result
        except Exception as exc:
            self._diag("search", error=str(exc), keyword=keyword, page=page)
            return self._empty_page(page, "搜索失败: %s" % exc)

    def _search_executor_instance(self):
        with self._search_executor_lock:
            if self._search_executor is None:
                self._search_executor = ThreadPoolExecutor(max_workers=2)
            return self._search_executor

    def _search_futures(self, cache_key, keyword, page):
        with self._search_inflight_lock:
            current = self._search_inflight.get(cache_key)
            if current:
                return current
            executor = self._search_executor_instance()
            futures = {
                executor.submit(self._search_primary, cache_key, keyword, page): "primary"
            }
            if page == 1:
                futures[executor.submit(self._search_suggestion_task, cache_key, keyword, page)] = "suggestion"
            self._search_inflight[cache_key] = futures
        for future in futures:
            future.add_done_callback(
                lambda _future, key=cache_key, group=futures: self._cleanup_search_inflight(key, group)
            )
        return futures

    def _search_primary(self, cache_key, keyword, page):
        params = {"q": keyword}
        if page > 1:
            params["p"] = page
        source, _ = self._request_text("/search", params=params)
        parsed = self._parse_search_page(source, page)
        if parsed.get("list"):
            self._remember_search(cache_key, parsed, 2)
        return parsed

    def _search_suggestion_task(self, cache_key, keyword, page):
        parsed = self._search_suggestions(keyword, page)
        if parsed.get("list"):
            self._remember_search(cache_key, parsed, 1)
        return parsed

    def _select_search_result(self, futures, page):
        pending = set(futures)
        best = None
        deadline = time.perf_counter() + max(8, self.timeout)
        suggestion_deadline = None
        while pending:
            active_deadline = deadline
            if suggestion_deadline is not None:
                active_deadline = min(active_deadline, suggestion_deadline)
            remaining = active_deadline - time.perf_counter()
            if remaining <= 0:
                break
            done, pending = wait(pending, timeout=remaining, return_when=FIRST_COMPLETED)
            if not done:
                break
            for future in done:
                kind = futures.get(future)
                try:
                    result = future.result()
                except Exception:
                    continue
                if not isinstance(result, dict) or not result.get("list"):
                    continue
                if kind == "primary":
                    return result
                best = result
                if suggestion_deadline is None:
                    suggestion_deadline = (
                        time.perf_counter() + self.search_quality_grace_ms / 1000.0
                    )
        return best or self._empty_page(page)

    def _cleanup_search_inflight(self, cache_key, futures):
        if not all(future.done() for future in futures):
            return
        with self._search_inflight_lock:
            if self._search_inflight.get(cache_key) is futures:
                self._search_inflight.pop(cache_key, None)

    def _schedule_search_refresh(self, cache_key, keyword, page):
        self._record_metric("search_refresh")
        self._search_futures(cache_key, keyword, page)

    def _cached_search(self, cache_key):
        state, result, _ = self._search_cache_state(cache_key)
        return result if state == "fresh" else None

    def _search_cache_state(self, cache_key):
        now = time.time()
        with self._cache_lock:
            cached = self._search_cache.get(cache_key)
            if not cached:
                return "miss", None, 0
            age = now - cached[0]
            if age > self.search_stale_ttl:
                self._search_cache.pop(cache_key, None)
                return "miss", None, age
            self._search_cache.move_to_end(cache_key)
            state = "fresh" if age <= self.search_cache_ttl else "stale"
            return state, cached[1], age

    def _remember_search(self, cache_key, result, rank):
        with self._cache_lock:
            current = self._search_cache.get(cache_key)
            if current and int(current[2]) > int(rank):
                return
            self._search_cache[cache_key] = (time.time(), result, int(rank))
            self._search_cache.move_to_end(cache_key)
            while len(self._search_cache) > self.MAX_CACHE:
                self._search_cache.popitem(last=False)

    def detailContent(self, ids):
        raw_id = ids[0] if isinstance(ids, (list, tuple)) and ids else ids
        value = str(raw_id or "").strip()
        if value.startswith("atvp_detail:"):
            value = value[len("atvp_detail:"):].strip()
        dir_name, item_id = self._decode_vod_id(value)
        if not dir_name or not item_id:
            return {"list": []}

        try:
            source, page_url = self._request_text("/%s/%s" % (dir_name, item_id))
            metadata = self._parse_detail_metadata(source, dir_name, item_id)
            resources = self._request_resources(dir_name, item_id)
            groups, stats = self._build_resource_groups(
                resources, dir_name, item_id, metadata.get("title") or ""
            )
            vod = self._build_detail_vod(metadata, groups, stats, dir_name, item_id)
            return {"list": [vod]}
        except Exception as exc:
            self._diag("detail", error=str(exc), dir=dir_name, id=item_id)
            return {
                "list": [
                    {
                        "vod_id": self._vod_id(dir_name, item_id),
                        "vod_name": "资源探测失败",
                        "vod_pic": self._image(dir_name, item_id, 384),
                        "vod_content": "详情读取失败: %s" % exc,
                        "vod_play_from": "诊断",
                        "vod_play_url": "查看错误$%s" % self._status_id(str(exc)),
                    }
                ]
            }

    def playerContent(self, flag, id, vipFlags):
        value = str(id or "").strip()
        if value.startswith(self.STATUS_PREFIX):
            message = self._decode_status(value)
            return self._player_error(message)
        payload = self._unpack(value)
        if not payload:
            return self._player_error("无法识别播放资源")
        kind = payload.get("kind")
        try:
            if kind == "pan":
                return self._play_pan(payload)
            if kind == "backend":
                return self._backend_play(str(payload.get("id") or ""))
            if kind == "online":
                return self._play_online(payload)
            if kind == "magnet":
                return self._play_magnet(payload)
            return self._player_error("未知资源类型: %s" % kind)
        except Exception as exc:
            self._diag("player", error=str(exc), kind=kind)
            return self._player_error("播放解析失败: %s" % exc)

    def _start_warmup(self):
        with self._warmup_guard:
            if self._warmup_started or self._warmup_event.is_set():
                return
            self._warmup_started = True
            generation = self._warmup_generation
        worker = threading.Thread(
            target=self._warmup_worker,
            args=(generation,),
            name="guanying-warmup",
        )
        worker.daemon = True
        worker.start()

    def _warmup_worker(self, generation):
        started = time.perf_counter()
        self._log("warmup", state="start")
        try:
            source, _ = self._request_text("/mv", params={"page": 1})
            parsed = self._parse_list_page(source, "mv", 1)
            if generation == self._warmup_generation:
                self._remember_page(("mv", 1), parsed)
            self._log(
                "warmup",
                state="ok",
                host=self._host or "none",
                count=len(parsed.get("list", [])),
                elapsed_ms=int((time.perf_counter() - started) * 1000),
            )
        except Exception as exc:
            self._diag("warmup", error=str(exc))
            self._log(
                "warmup",
                state="error",
                error=type(exc).__name__,
                elapsed_ms=int((time.perf_counter() - started) * 1000),
            )
        finally:
            with self._warmup_guard:
                if generation == self._warmup_generation:
                    self._warmup_event.set()
            self._start_standby_warmup(generation)

    def _cached_page(self, key):
        state, value, _ = self._page_cache_state(key)
        return value if state == "fresh" else None

    def _page_cache_state(self, key):
        now = time.time()
        with self._cache_lock:
            cached = self._page_cache.get(key)
            if cached is None:
                return "miss", None, 0
            if (
                isinstance(cached, (tuple, list))
                and len(cached) == 2
                and isinstance(cached[0], (int, float))
            ):
                cached_at, value = cached
            else:
                cached_at, value = now, cached
            age = now - cached_at
            if age > self.category_stale_ttl:
                self._page_cache.pop(key, None)
                return "miss", None, age
            self._page_cache.move_to_end(key)
            state = "fresh" if age <= self.category_cache_ttl else "stale"
            return state, value, age

    def _refresh_executor_instance(self):
        with self._refresh_executor_lock:
            if self._refresh_executor is None:
                self._refresh_executor = ThreadPoolExecutor(max_workers=2)
            return self._refresh_executor

    def _schedule_category_refresh(self, cache_key, category, page):
        with self._search_inflight_lock:
            if cache_key in self._refresh_inflight:
                return
            self._refresh_inflight.add(cache_key)
        self._record_metric("category_refresh")
        future = self._refresh_executor_instance().submit(
            self._refresh_category_page, cache_key, category, page
        )
        future.add_done_callback(
            lambda _future, key=cache_key: self._finish_category_refresh(key)
        )

    def _refresh_category_page(self, cache_key, category, page):
        try:
            source, _ = self._request_text("/%s" % category, params={"page": page})
            parsed = self._parse_list_page(source, category, page)
            self._remember_page(cache_key, parsed)
            self._record_metric("category_refresh_ok")
        except Exception as exc:
            self._record_metric("category_refresh_error")
            self._log("category-refresh", state="error", error=type(exc).__name__)

    def _finish_category_refresh(self, cache_key):
        with self._search_inflight_lock:
            self._refresh_inflight.discard(cache_key)

    def _fetch_category_page(self, category, page):
        cache_key = (category, page)
        cached = self._cached_page(cache_key)
        if cached is not None:
            return cached
        source, _ = self._request_text("/%s" % category, params={"page": page})
        parsed = self._parse_list_page(source, category, page)
        self._remember_page(cache_key, parsed)
        return parsed

    def _request_text(self, path, params=None, retry=True):
        last_error = None
        attempts = 2 if retry else 1
        for _ in range(attempts):
            try:
                return self._hedged_site_call(
                    "text",
                    lambda host, session: self._get_text_from_host(
                        host, session, path, params
                    ),
                )
            except Exception as exc:
                last_error = exc
        raise RuntimeError(str(last_error or "请求失败"))

    def _get_text_from_host(self, host, session, path, params):
        url = urljoin(host + "/", str(path or "").lstrip("/"))
        response = session.get(
            url,
            params=params,
            headers={"Referer": host + "/"},
            timeout=self.timeout,
        )
        if self._is_challenge(response.text) or self._is_login_page(response.text):
            raise RuntimeError("登录状态失效")
        response.raise_for_status()
        response.encoding = response.encoding or "utf-8"
        return response.text, response.url

    def _request_resources(self, dir_name, item_id, retry=True):
        key = (dir_name, item_id)
        with self._cache_lock:
            cached = self._resource_cache.get(key)
            if cached is not None:
                self._resource_cache.move_to_end(key)
                return cached
        last_error = None
        attempts = 2 if retry else 1
        data = None
        for _ in range(attempts):
            try:
                data = self._hedged_site_call(
                    "resources",
                    lambda host, session: self._get_resources_from_host(
                        host, session, dir_name, item_id
                    ),
                )
                break
            except Exception as exc:
                last_error = exc
        if data is None:
            raise RuntimeError(str(last_error or "资源请求失败"))
        with self._cache_lock:
            self._resource_cache[key] = data
            self._resource_cache.move_to_end(key)
            while len(self._resource_cache) > self.MAX_CACHE:
                self._resource_cache.popitem(last=False)
        return data

    def _get_resources_from_host(self, host, session, dir_name, item_id):
        url = "%s/res/downurl/%s/%s" % (host, quote(dir_name), quote(item_id))
        response = session.get(
            url,
            headers={
                "Referer": "%s/%s/%s" % (host, dir_name, item_id),
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=self.timeout,
        )
        if self._is_challenge(response.text) or self._is_login_page(response.text):
            raise RuntimeError("资源接口要求重新登录")
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or int(data.get("code") or 0) != 200:
            raise RuntimeError(str(data.get("msg") or "资源接口返回异常"))
        self._validate_resource_payload(data)
        return data

    def _hedge_executor_instance(self):
        with self._hedge_executor_lock:
            if self._hedge_executor is None:
                # Search can issue a primary and suggestion request together;
                # four workers leave room for one delayed mirror per request.
                self._hedge_executor = ThreadPoolExecutor(max_workers=4)
            return self._hedge_executor

    def _hedged_site_call(self, action, operation):
        primary_host, primary_session = self._client()
        alternate = self._ready_alternate_client(primary_host)
        started = time.perf_counter()
        if not self.enable_hedged_requests or alternate is None:
            try:
                result = operation(primary_host, primary_session)
                self._record_metric("site_request_ok", elapsed_ms=self._elapsed_ms(started))
                return result
            except Exception:
                self._record_metric("site_request_error")
                self._invalidate_host(primary_host)
                raise

        executor = self._hedge_executor_instance()
        primary_future = executor.submit(operation, primary_host, primary_session)
        done, pending = wait(
            {primary_future},
            timeout=self.hedge_delay_ms / 1000.0,
            return_when=FIRST_COMPLETED,
        )
        errors = []
        if done:
            try:
                result = primary_future.result()
                self._record_metric("hedge_primary_fast")
                self._record_metric("site_request_ok", elapsed_ms=self._elapsed_ms(started))
                return result
            except Exception as exc:
                errors.append(exc)
                self._invalidate_host(primary_host)

        alternate_host, alternate_session = alternate
        self._record_metric("hedge_launched")
        self._log("mirror-hedge", state="launched", action=action)
        alternate_future = executor.submit(operation, alternate_host, alternate_session)
        future_hosts = {
            primary_future: primary_host,
            alternate_future: alternate_host,
        }
        pending = {future for future in future_hosts if not future.done()}
        if primary_future.done() and not done:
            pending.add(primary_future)
        deadline = time.perf_counter() + max(self.timeout + 2, 10)
        while pending:
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                break
            completed, pending = wait(
                pending, timeout=remaining, return_when=FIRST_COMPLETED
            )
            if not completed:
                break
            for future in completed:
                host = future_hosts[future]
                try:
                    result = future.result()
                except Exception as exc:
                    errors.append(exc)
                    self._invalidate_host(host)
                    continue
                self._activate_host(host)
                metric = "hedge_primary_win" if host == primary_host else "hedge_mirror_win"
                self._record_metric(metric)
                self._record_metric("site_request_ok", elapsed_ms=self._elapsed_ms(started))
                for other in pending:
                    other.cancel()
                self._log("mirror-hedge", state="winner", action=action, mirror=int(host != primary_host))
                return result
        self._record_metric("site_request_error")
        raise errors[-1] if errors else RuntimeError("镜像竞速请求超时")

    def _ready_alternate_client(self, primary_host):
        with self._lock:
            ordered = []
            for item in self._last_candidates:
                if len(item) >= 3:
                    ordered.append(item[2])
            ordered.extend(self._sessions)
            for host in ordered:
                normalized = self._normalize_host(host)
                if (
                    not normalized
                    or normalized == primary_host
                    or self._host_in_cooldown(normalized)
                ):
                    continue
                session = self._sessions.get(normalized)
                if session is not None:
                    return normalized, session
        return None

    def _activate_host(self, host):
        normalized = self._normalize_host(host)
        if not normalized:
            return
        with self._lock:
            self._host = normalized
            self._host_checked_at = time.time()

    def _client(self):
        with self._lock:
            force_probe = self.probe_each_request
            expired = time.time() - self._host_checked_at >= self.probe_ttl
            if not self._host or force_probe or expired:
                self._select_host()
            last_error = None
            for _ in range(3):
                host = self._host
                if not host:
                    self._select_host()
                    host = self._host
                session = self._sessions.get(host)
                if session is None:
                    try:
                        session = self._authenticate(host)
                    except Exception as exc:
                        last_error = exc
                        self._invalidate_host(host)
                        self._select_host()
                        continue
                    self._sessions[host] = session
                    self._session_times[host] = time.time()
                return host, session
            raise RuntimeError(str(last_error or "没有可用镜像"))

    def _start_standby_warmup(self, generation):
        if not self.enable_hedged_requests or not self.warm_standby_mirror:
            return
        with self._standby_lock:
            if generation != self._warmup_generation or self._standby_generation == generation:
                return
            self._standby_generation = generation
        worker = threading.Thread(
            target=self._standby_warmup_worker,
            args=(generation,),
            name="guanying-standby-mirror",
        )
        worker.daemon = True
        worker.start()

    def _standby_warmup_worker(self, generation):
        with self._lock:
            current = self._host
            candidates = [
                item[2] for item in self._last_candidates if len(item) >= 3
            ]
        if not candidates:
            try:
                discovered = [
                    item for item in self._discover_candidates()
                    if not self._host_in_cooldown(item[1])
                ][: self.max_probe_hosts]
                checked = []
                if discovered:
                    workers = min(self.max_probe_hosts, len(discovered))
                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        futures = {
                            executor.submit(self._probe_host, host, declared): (declared, host)
                            for declared, host in discovered
                        }
                        for future in as_completed(futures):
                            declared, host = futures[future]
                            latency = future.result()
                            if latency is not None:
                                checked.append((declared, latency, host))
                checked.sort(key=lambda item: (item[1], item[0]))
                candidates = [item[2] for item in checked]
                if checked:
                    with self._lock:
                        if not self._last_candidates:
                            self._last_candidates = checked
            except Exception as exc:
                self._log("standby-mirror", state="discover-error", error=type(exc).__name__)
        for host in candidates:
            normalized = self._normalize_host(host)
            if (
                not normalized
                or normalized == current
                or self._host_in_cooldown(normalized)
            ):
                continue
            with self._lock:
                if normalized in self._sessions:
                    return
            session = None
            try:
                session = self._authenticate(normalized)
                with self._warmup_guard:
                    active = generation == self._warmup_generation
                if not active:
                    session.close()
                    return
                with self._lock:
                    existing = self._sessions.get(normalized)
                    if existing is None:
                        self._sessions[normalized] = session
                        self._session_times[normalized] = time.time()
                        session = None
                self._record_metric("standby_mirror_ready")
                self._log("standby-mirror", state="ready")
                return
            except Exception as exc:
                self._mark_host_failed(normalized)
                self._log("standby-mirror", state="error", error=type(exc).__name__)
            finally:
                if session is not None:
                    try:
                        session.close()
                    except Exception:
                        pass

    def _select_host(self):
        started = time.perf_counter()
        candidates = self._discover_candidates()
        if not candidates:
            raise RuntimeError("发布页没有可用镜像")
        now = time.time()
        eligible = [
            item for item in candidates
            if not self._host_in_cooldown(item[1], now)
        ]
        if not eligible:
            self._failed_hosts.clear()
            self._failed_until.clear()
            eligible = candidates
        checked = []
        workers = min(self.max_probe_hosts, len(eligible))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._probe_host, host, declared): (declared, host)
                for declared, host in eligible[: self.max_probe_hosts]
            }
            for future in as_completed(futures):
                declared, host = futures[future]
                result = future.result()
                if result is None:
                    self._mark_host_failed(host)
                    continue
                checked.append((declared, result, host))
        checked.sort(key=lambda item: (item[1], item[0]))
        selected = checked[0][2] if checked else None
        # Keep a healthy current line when it is close to the fastest result;
        # this avoids throwing away a logged-in session on small jitter.
        if self._host:
            current = next((item for item in checked if item[2] == self._host), None)
            if current and selected and current[1] <= checked[0][1] * 1.35 + 80:
                selected = self._host
        if not selected:
            selected = eligible[0][1]
        if selected != self._host:
            self._host = selected
        self._host_checked_at = time.time()
        self._last_candidates = checked or [(latency, 0, host) for latency, host in candidates]
        self._diag("host", host=selected, candidates=self._last_candidates[:10])
        self._log(
            "host",
            state="selected",
            host=selected,
            reachable=len(checked),
            elapsed_ms=int((time.perf_counter() - started) * 1000),
        )

    def _probe_host(self, host, declared_latency=99999):
        started = time.perf_counter()
        session = self._new_session()
        try:
            response = session.get(
                host + "/",
                timeout=self.probe_timeout,
                allow_redirects=True,
            )
            if response.status_code >= 500:
                return None
            return max(1, int((time.perf_counter() - started) * 1000))
        except Exception:
            return None
        finally:
            try:
                session.close()
            except Exception:
                pass

    def _discover_candidates(self):
        result = []
        pages = list(self.address_pages or [self.address_page])
        remote_config = self._load_mirror_config()
        pages.extend(remote_config.get("pages") or [])
        errors = []
        for page in self._http_url_list(pages):
            try:
                session = self._new_session()
                response = session.get(
                    page,
                    timeout=self.probe_timeout,
                    allow_redirects=True,
                )
                response.raise_for_status()
                response.encoding = response.encoding or "utf-8"
                source = response.text
                final_url = response.url
                refresh = re.search(
                    r"http-equiv=['\"]refresh['\"][^>]+content=['\"][^;]+;\s*url=([^'\">]+)",
                    source,
                    re.I,
                )
                if refresh:
                    target = urljoin(final_url, html_lib.unescape(refresh.group(1).strip()))
                    response = session.get(target, timeout=self.probe_timeout + 5)
                    response.raise_for_status()
                    response.encoding = response.encoding or "utf-8"
                    source = response.text
                    final_url = response.url
                result.extend(self._parse_publish_page(source, final_url))
                session.close()
            except Exception as exc:
                errors.append("%s: %s" % (page, exc))
        fallback_hosts = self._url_list(
            list(self.fallback_hosts) + list(remote_config.get("hosts") or [])
        )
        known = [(99999 + index, host) for index, host in enumerate(fallback_hosts)]
        if result:
            merged = self._dedupe_candidates(result + known)
            with self._cache_lock:
                self._candidate_cache = merged
                self._candidate_cached_at = time.time()
            return merged
        with self._cache_lock:
            cached = list(self._candidate_cache)
            cached_at = self._candidate_cached_at
        if cached and time.time() - cached_at < 86400:
            self._diag("publish-cache", age=int(time.time() - cached_at), errors=errors[:3])
            return self._dedupe_candidates(cached + known)
        self._diag("publish", error="; ".join(errors[:3]))
        return known

    def _load_mirror_config(self):
        if not self.mirror_config_url:
            return {"pages": [], "hosts": []}
        try:
            session = self._new_session()
            response = session.get(self.mirror_config_url, timeout=self.probe_timeout)
            response.raise_for_status()
            data = response.json()
            session.close()
            if isinstance(data, list):
                return {"pages": [], "hosts": self._url_list(data)}
            if isinstance(data, dict):
                return {
                    "pages": self._http_url_list(data.get("pages") or data.get("address_pages") or []),
                    "hosts": self._url_list(data.get("hosts") or data.get("fallback_hosts") or []),
                }
        except Exception as exc:
            self._diag("mirror-config", error=str(exc))
        return {"pages": [], "hosts": []}

    def _parse_publish_page(self, source, page_url):
        try:
            document = html.fromstring(source)
            document.make_links_absolute(page_url)
        except Exception:
            return []
        candidates = []
        for anchor in document.xpath("//a[@href]"):
            href = self._normalize_host(anchor.get("href"))
            text = self._clean_text(anchor.text_content())
            if not href or "无法访问" in text or "不可访问" in text:
                continue
            match = re.search(r"(\d+)\s*ms", text, re.I)
            latency = int(match.group(1)) if match else 99990
            candidates.append((latency, href))
        candidates.sort(key=lambda item: item[0])
        return candidates

    def _authenticate(self, host):
        if not self.username or not self.password:
            raise RuntimeError("未配置站点登录账号")
        started = time.perf_counter()
        self._log("auth", state="start", host=host)
        session = self._new_session()
        session.headers.update(dict(self.headers))
        session.get(host + "/", timeout=self.timeout)
        self._solve_pow(session, host)
        payload = {
            "code": "",
            "siteid": "1",
            "dosubmit": "1",
            "cookietime": "10506240",
            "username": self.username,
            "password": self.password,
        }
        response = session.post(
            host + "/user/login",
            data=payload,
            headers={
                "Referer": host + "/user/login",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        try:
            result = response.json()
        except Exception as exc:
            raise RuntimeError("登录接口不是 JSON: %s" % exc)
        if int(result.get("code") or 0) != 200:
            raise RuntimeError(str(result.get("msg") or "登录失败"))
        self._log(
            "auth",
            state="ok",
            host=host,
            elapsed_ms=int((time.perf_counter() - started) * 1000),
        )
        return session

    def _new_session(self):
        session = requests.Session()
        # Reachability must be measured from the container itself, not through
        # a desktop proxy inherited from HTTP(S)_PROXY environment variables.
        session.trust_env = not self.direct_only
        session.headers.update(dict(self.headers))
        return session

    def _backend_session(self):
        """Reuse one HTTP connection per worker thread for parse/play calls."""
        thread_id = threading.get_ident()
        with self._backend_session_lock:
            session = self._backend_sessions.get(thread_id)
            if session is None:
                session = self._new_session()
                self._backend_sessions[thread_id] = session
            return session

    def _close_backend_sessions(self):
        with self._backend_session_lock:
            sessions = list(self._backend_sessions.values())
            self._backend_sessions.clear()
        for session in sessions:
            try:
                session.close()
            except Exception:
                pass

    def _solve_pow(self, session, host):
        response = session.get(host + "/res/pow", timeout=self.timeout)
        response.raise_for_status()
        challenge = response.json()
        modulus = int(str(challenge["N"]), 16)
        value = int(str(challenge["x"]), 16)
        steps = int(challenge["t"])
        if steps < 1 or steps > 2000000:
            raise RuntimeError("PoW 步数异常: %s" % steps)
        started = time.perf_counter()
        for _ in range(steps):
            value = (value * value) % modulus
        elapsed = time.perf_counter() - started
        if elapsed < 3:
            time.sleep(3 - elapsed)
        verify = session.post(
            host + "/res/pow",
            data={"y": format(value, "x")},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self.timeout,
        )
        verify.raise_for_status()
        data = verify.json()
        if not data.get("success"):
            raise RuntimeError("浏览器计算验证失败")

    def _invalidate_host(self, host):
        with self._lock:
            normalized = self._normalize_host(host)
            if normalized:
                self._mark_host_failed(normalized)
                session = self._sessions.pop(normalized, None)
                if session:
                    try:
                        session.close()
                    except Exception:
                        pass
            if self._host == normalized:
                self._host = ""
                self._host_checked_at = 0.0

    def _mark_host_failed(self, host):
        normalized = self._normalize_host(host)
        if not normalized:
            return
        key = normalized.lower()
        self._failed_hosts.add(key)
        self._failed_until[key] = time.time() + self.host_fail_cooldown

    def _host_in_cooldown(self, host, now=None):
        key = self._normalize_host(host).lower()
        if not key:
            return True
        until = self._failed_until.get(key, 0)
        if until and (now or time.time()) >= until:
            self._failed_until.pop(key, None)
            self._failed_hosts.discard(key)
            return False
        return until > (now or time.time())

    def _parse_list_page(self, source, category, page):
        data = self._extract_obj(source, "inlist")
        if not isinstance(data, dict):
            raise RuntimeError("未找到列表数据")
        page_data = self._extract_obj(source, "page") or {}
        ids = self._array(data, "i", "id", "ids")
        titles = self._array(data, "t", "title", "titles")
        dirs = self._array(data, "d", "dir", "dirs")
        qualities = self._array(data, "q", "quality", "qualities")
        magnet_counts = self._array(data, "b", "bt", "magnet")
        pan_counts = self._array(data, "w", "pan")
        online_counts = self._array(data, "z", "play", "online")
        default_dir = str(data.get("ty") or category.split("/")[-1])
        videos = []
        for index, item_id in enumerate(ids):
            title = self._at(titles, index, "")
            dir_name = self._at(dirs, index, default_dir)
            if dir_name not in ("mv", "tv", "ac"):
                dir_name = default_dir if default_dir in ("mv", "tv", "ac") else "mv"
            quality = self._at(qualities, index, [])
            remark = self._resource_remark(
                quality,
                self._at(magnet_counts, index, 0),
                self._at(pan_counts, index, 0),
                self._at(online_counts, index, 0),
            )
            if not item_id or not title:
                continue
            videos.append(
                self._vod_card(
                    dir_name,
                    item_id,
                    self._clean_text(title),
                    self._image(dir_name, item_id),
                    remark,
                )
            )
        pagecount = self._bounded_int(page_data.get("pages"), page, page, 10000)
        return {
            "list": videos,
            "page": page,
            "pagecount": pagecount,
            "limit": len(videos) or 48,
            "total": pagecount * (len(videos) or 48),
        }

    def _parse_search_page(self, source, page):
        page_data = self._extract_obj(source, "page") or {}
        assignments = self._extract_all_objs(source)
        data = None
        for key, value in assignments.items():
            if key in ("header", "page", "footer") or not isinstance(value, dict):
                continue
            candidates = [value]
            nested = value.get("l")
            if isinstance(nested, dict):
                candidates.insert(0, nested)
            for candidate in candidates:
                if self._array(candidate, "i", "id", "ids") and self._array(
                    candidate, "title", "t", "titles"
                ):
                    data = candidate
                    break
            if data is not None:
                break
        if not data:
            return self._empty_page(page)
        ids = self._array(data, "i", "id", "ids")
        titles = self._array(data, "title", "t", "titles")
        dirs = self._array(data, "d", "dir", "dirs")
        years = self._array(data, "year", "years")
        infos = self._array(data, "info", "infos")
        videos = []
        for index, item_id in enumerate(ids):
            dir_name = str(self._at(dirs, index, "mv"))
            if dir_name not in ("mv", "tv", "ac"):
                dir_name = "mv"
            title = self._clean_text(self._at(titles, index, ""))
            if not item_id or not title:
                continue
            year = self._at(years, index, "")
            info = self._clean_text(self._at(infos, index, ""))
            remark = " / ".join([str(v) for v in (year, info) if v])[:80]
            videos.append(
                self._vod_card(
                    dir_name,
                    item_id,
                    title,
                    self._image(dir_name, item_id),
                    remark,
                )
            )
        pagecount = self._bounded_int(page_data.get("pages"), page, page, 10000)
        return {
            "list": videos,
            "page": page,
            "pagecount": pagecount,
            "limit": len(videos) or 25,
            "total": pagecount * (len(videos) or 25),
        }

    def _search_suggestions(self, keyword, page=1):
        host, session = self._client()
        response = session.get(
            host + "/res/search_suggest",
            params={"q": keyword},
            headers={
                "Referer": host + "/",
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=self.timeout,
        )
        if self._is_challenge(response.text) or self._is_login_page(response.text):
            raise RuntimeError("搜索联想接口要求重新登录")
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            return self._empty_page(page)
        videos = []
        seen = set()
        for item in data:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("id") or "").strip()
            title = self._clean_text(item.get("title") or "")
            dir_name = str(item.get("dir") or "mv").strip()
            if dir_name not in ("mv", "tv", "ac"):
                dir_name = "mv"
            marker = (dir_name, item_id)
            if not item_id or not title or marker in seen:
                continue
            seen.add(marker)
            remark = " / ".join(
                value
                for value in (
                    self._clean_text(item.get("year") or ""),
                    self._clean_text(item.get("type") or ""),
                )
                if value
            )[:80]
            videos.append(
                self._vod_card(
                    dir_name,
                    item_id,
                    title,
                    self._image(dir_name, item_id),
                    remark,
                )
            )
        return {
            "list": videos,
            "page": page,
            "pagecount": page,
            "limit": len(videos) or 25,
            "total": len(videos),
        }

    def _parse_detail_metadata(self, source, dir_name, item_id):
        data = self._extract_obj(source, "d")
        if not isinstance(data, dict):
            raise RuntimeError("未找到详情元数据")
        title = self._clean_text(data.get("title") or "未知影片")
        year = str(data.get("year") or "")
        actors = self._join_text(data.get("zhuyan"))
        directors = self._join_text(data.get("daoyan"))
        genres = self._join_text(data.get("leixing"))
        areas = self._join_text(data.get("diqu"))
        remarks = self._clean_text(re.sub(r"<[^>]+>", "", str(data.get("status") or "")))
        content = self._clean_text(data.get("summary") or "")
        return {
            "title": title,
            "year": year,
            "actors": actors,
            "directors": directors,
            "genres": genres,
            "areas": areas,
            "remarks": remarks,
            "content": content,
            "pic": self._image(dir_name, item_id, 384),
        }

    def _build_resource_groups(self, data, dir_name, item_id, title=""):
        groups = []
        magnets = self._extract_magnets(data.get("downlist") or {}, dir_name, item_id)
        raw_magnet_count = len(magnets)
        magnet_episodes_expanded = False
        online = self._extract_online(data.get("playlist") or [], dir_name, item_id)
        if not self.enable_online_sources:
            online = []
        online_limit = self._online_episode_limit(online)
        if magnets and online_limit and online_limit > 1:
            magnets = self._expand_magnet_episodes(
                magnets, self._season_number(title), online_limit
            )
            magnet_episodes_expanded = True
        pans = self._extract_pans(
            data.get("panlist") or {}, dir_name, item_id, online_limit
        )
        pans = {
            provider: items
            for provider, items in pans.items()
            if self._provider_allowed(provider)
        }
        pan_progress = {"ready": 0, "pending": 0, "total": 0}
        if self.expand_pan_playlists and dir_name in ("tv", "ac"):
            pans, pan_progress = self._expand_pan_groups(
                pans, self._season_number(title), online_limit
            )

        magnet_group = None
        if magnets:
            magnet_group = (
                "磁力资源",
                magnets if magnet_episodes_expanded else magnets[: self.max_magnets],
            )
        elif self.show_empty_magnet_group:
            magnet_group = (
                "磁力资源",
                [("本片暂无磁力资源", self._status_id("本片暂无磁力资源"))],
            )
        providers = list(self.PROVIDER_ORDER)
        providers.extend(sorted(name for name in pans if name not in providers))
        for provider in providers:
            items = pans.get(provider) or []
            if items:
                groups.append((provider, items[: self.max_per_pan]))
        if magnet_group:
            groups.append(magnet_group)
        for name, items in online[: self.max_online_groups]:
            groups.append((name, items))

        stats = {
            "magnet": raw_magnet_count,
            "pan": sum(len(items) for items in pans.values()),
            "online": sum(len(items) for _, items in online),
            "groups": len(groups),
            "pan_ready": pan_progress.get("ready", 0),
            "pan_pending": pan_progress.get("pending", 0),
            "pan_total": pan_progress.get("total", 0),
        }
        if not groups:
            groups.append(("诊断", [("未探测到可用资源", self._status_id("资源接口为空"))]))
        return groups, stats

    def _extract_magnets(self, downlist, dir_name, item_id):
        listing = downlist.get("list") if isinstance(downlist, dict) else {}
        if not isinstance(listing, dict):
            return []
        magnets = self._array(listing, "m", "magnet", "hash")
        titles = self._array(listing, "t", "title", "name")
        sizes = self._array(listing, "s", "size")
        seeds = self._array(listing, "e", "seeds")
        seen = set()
        items = []
        for index, raw in enumerate(magnets):
            magnet = self._normalize_magnet(str(raw or ""), self._at(titles, index, ""))
            info_hash = self._btih(magnet)
            if not magnet or not info_hash or info_hash in seen:
                continue
            seen.add(info_hash)
            title = self._clean_text(self._at(titles, index, "磁力%s" % info_hash[:8]))
            size = self._clean_text(self._at(sizes, index, ""))
            seed = self._safe_int(self._at(seeds, index, 0))
            label = self._safe_label(title)
            if size:
                label = self._safe_label("%s [%sB]" % (label, size))
            if seed > 0:
                label = self._safe_label("%s 种%s" % (label, seed))
            payload = {
                "kind": "magnet",
                "magnet": magnet,
            }
            items.append(
                {
                    "label": label,
                    "id": self._pack(payload),
                    "subtitle": bool(self.SUBTITLE_RE.search(title)),
                    "quality": self._quality_score(title),
                    "negative": bool(self.NEGATIVE_RE.search(title)),
                    "seeds": seed,
                    "size": self._size_bytes(size),
                    "order": index,
                }
            )
        items.sort(
            key=lambda item: (
                1 if item["subtitle"] else 0,
                0 if item["negative"] else 1,
                item["quality"],
                item["seeds"],
                item["size"],
                -item["order"],
            ),
            reverse=True,
        )
        return [(item["label"], item["id"]) for item in items]

    def _extract_pans(self, panlist, dir_name, item_id, episode_limit=None):
        if not isinstance(panlist, dict):
            return {}
        urls = self._array(panlist, "url", "urls")
        names = self._array(panlist, "name", "title", "titles")
        passwords = self._array(panlist, "p", "password", "pwd")
        types = self._array(panlist, "type", "types")
        times = self._array(panlist, "time", "times")
        type_names = self._array(panlist, "tname", "type_name", "type_names")
        groups = {name: [] for name in self.PROVIDER_ORDER}
        seen = set()
        for index, raw_url in enumerate(urls):
            url = html_lib.unescape(str(raw_url or "")).strip()
            if not url.startswith(("http://", "https://")):
                continue
            normalized = url.rstrip("#")
            if normalized in seen:
                continue
            seen.add(normalized)
            name = self._clean_text(self._at(names, index, "网盘资源"))
            password = self._clean_text(self._at(passwords, index, ""))
            provider = self._provider_name(
                url,
                self._at(types, index, None),
                type_names,
            )
            label = self._safe_label(name)
            if password and password.lower() not in url.lower():
                label = self._safe_label("%s 提取码:%s" % (label, password))
            high_bitrate = bool(
                re.search(r"高码(?:率)?|REMUX|BluRay|BDREMUX|WEB[- .]?DL", name, re.I)
            )
            quality = self._quality_score(name)
            size = self._size_bytes(name)
            declared_episodes = self._declared_episode_count(name)
            final_url = self._append_pan_password(url, password)
            if not self._pan_link_available(final_url):
                continue
            profile = {
                "url": final_url,
                "episode_count": declared_episodes,
                "quality": quality,
                "high_bitrate": high_bitrate,
                "max_size": size,
            }
            payload = {
                "kind": "pan",
                "url": final_url,
                "provider": provider,
                "profile": profile,
            }
            groups.setdefault(provider, []).append(
                {
                    "label": label,
                    "id": self._pack(payload),
                    "subtitle": bool(self.SUBTITLE_RE.search(name)),
                    "high_bitrate": high_bitrate,
                    "quality": quality,
                    "size": size,
                    "episode_count": declared_episodes,
                    "negative": bool(self.NEGATIVE_RE.search(name)),
                    "time": self._time_score(self._at(times, index, "")),
                    "order": index,
                }
            )
        for provider, items in groups.items():
            items.sort(
                key=lambda item: (
                    0 if item["negative"] else 1,
                    1 if item["high_bitrate"] else 0,
                    item["quality"],
                    1 if episode_limit and item["episode_count"] >= episode_limit else 0,
                    item["episode_count"],
                    item["size"],
                    1 if item["subtitle"] else 0,
                    item["time"],
                    -item["order"],
                ),
                reverse=True,
            )
            groups[provider] = [(item["label"], item["id"]) for item in items]
        return groups

    def _declared_episode_count(self, value):
        text = self._clean_text(value)
        counts = []
        for pattern in (
            r"(?:全|共)?\s*(\d{1,3})\s*集",
            r"(?:第?\s*)?\d{1,3}\s*[-~至到]\s*(\d{1,3})(?:\s*集)?",
            r"(?:EP?|E)\s*0*1\s*[-~至到]\s*(?:EP?|E)?\s*0*(\d{1,3})",
        ):
            counts.extend(int(match) for match in re.findall(pattern, text, re.I))
        return max((count for count in counts if 0 < count <= 500), default=0)

    def _append_pan_password(self, url, password):
        value = str(url or "").strip()
        pwd = self._clean_text(password)
        if not value or not pwd:
            return value
        parts = urlsplit(value)
        query = parse_qsl(parts.query, keep_blank_values=True)
        if any(key.lower() in ("pwd", "password", "passcode", "code") for key, _ in query):
            return value
        query.append(("pwd", pwd))
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))

    def _expand_magnet_episodes(self, items, target_season, episode_limit):
        magnets = []
        for _, packed_id in items:
            payload = self._unpack(packed_id) or {}
            magnet = self._normalize_magnet(payload.get("magnet"))
            if magnet and magnet not in magnets:
                magnets.append(magnet)
            if len(magnets) >= self.magnet_fallback_links:
                break
        if not magnets:
            return items
        expanded = []
        for episode in range(1, min(500, episode_limit) + 1):
            payload = {
                "kind": "magnet",
                "magnet": magnets[0],
                "magnets": magnets,
                "season": target_season,
                "episode": episode,
                "episode_limit": episode_limit,
            }
            expanded.append(("第%s集" % episode, self._pack(payload)))
        return expanded

    def _online_episode_limit(self, online):
        limit = 0
        for _, items in online or []:
            for _, packed_id in items:
                payload = self._unpack(packed_id) or {}
                limit = max(limit, self._safe_int(payload.get("episode")))
        return limit or None

    def _expand_pan_groups(self, pans, target_season=None, episode_limit=None):
        if not pans or not self.alist_api or not self.alist_token:
            return pans, {"ready": 0, "pending": 0, "total": len(pans or {})}
        provider_jobs = []
        for provider in self.PROVIDER_ORDER + tuple(sorted(pans)):
            if not self._provider_allowed(provider):
                continue
            items = pans.get(provider) or []
            if not items or any(name == provider for name, _ in provider_jobs):
                continue
            urls = []
            for label, packed_id in items:
                payload = self._unpack(packed_id)
                url = str((payload or {}).get("url") or "")
                if url and url not in urls:
                    urls.append(url)
                if len(urls) >= self.pan_expand_attempts:
                    break
            if urls:
                provider_jobs.append((provider, urls))
        if not provider_jobs:
            return {}, {"ready": 0, "pending": 0, "total": 0}
        _, dynamic_job_limit = self._pan_prewarm_limits(len(provider_jobs))

        provider_candidates = {}
        missing = []
        for provider, urls in provider_jobs:
            for url in urls:
                found, candidates = self._cached_backend_candidates(url)
                if found and candidates:
                    provider_candidates.setdefault(provider, []).extend(
                        dict(candidate, _resource_url=url) for candidate in candidates
                    )
                else:
                    if not found:
                        missing.append((provider, url))

        # Detail must remain a cheap, lazy operation.  A cached parse can enrich
        # the labels immediately, but a cache miss is always scheduled in the
        # background; doing the first /parse synchronously makes the client wait
        # on every detail entry and encourages it to auto-select the first item.
        missing_set = set(missing)
        background_jobs = []
        for candidate_index in range(self.pan_expand_attempts):
            for provider, urls in provider_jobs:
                if candidate_index >= len(urls):
                    continue
                job = (provider, urls[candidate_index])
                if job in missing_set:
                    background_jobs.append(job)
                if len(background_jobs) >= dynamic_job_limit:
                    break
            if len(background_jobs) >= dynamic_job_limit:
                break
        self._schedule_pan_background(background_jobs)

        result = {}
        ready = 0
        for provider, urls in provider_jobs:
            episode_map = self._episode_candidates(
                provider_candidates.get(provider) or [], target_season, episode_limit
            )
            if episode_map:
                ready += 1
                items = []
                for episode in sorted(episode_map):
                    candidate = episode_map[episode]
                    quality = self._quality_label(candidate.get("name"))
                    label = "第%s集%s" % (episode, (" · " + quality) if quality else "")
                    resource_url = str(candidate.get("_resource_url") or "").strip()
                    raw_items = pans.get(provider) or []
                    lazy_payload = self._lazy_pan_payload(
                        raw_items,
                        resource_url,
                        target_season,
                        episode,
                        episode_limit,
                    )
                    items.append(
                        (
                            self._safe_label(label),
                            self._pack(lazy_payload),
                        )
                    )
                result[provider] = items
                continue
            raw_items = pans.get(provider) or []
            if raw_items and episode_limit and episode_limit <= 500:
                placeholders = []
                for episode in range(1, episode_limit + 1):
                    episode_payload = self._lazy_pan_payload(
                        raw_items,
                        "",
                        target_season,
                        episode,
                        episode_limit,
                    )
                    # Keep the explicit assignment visible to raw ATVP
                    # compatibility tooling and preserve the selected slot.
                    episode_payload["episode"] = episode
                    placeholders.append(
                        (
                            "第%s集" % episode,
                            self._pack(episode_payload),
                        )
                    )
                result[provider] = placeholders
        total = len(provider_jobs)
        progress = {"ready": ready, "pending": max(0, total - ready), "total": total}
        self._log("pan-progress", **progress)
        return result, progress

    def _lazy_pan_payload(
        self, raw_items, preferred_url, target_season, episode, episode_limit
    ):
        payloads = []
        for _, packed_id in raw_items or []:
            payload = self._unpack(packed_id) or {}
            url = str(payload.get("url") or "").strip()
            if not url or any(item[0] == url for item in payloads):
                continue
            payloads.append((url, payload))
            if len(payloads) >= self.pan_fallback_links:
                break
        preferred = str(preferred_url or "").strip()
        if preferred:
            payloads.sort(key=lambda item: 0 if item[0] == preferred else 1)
        base = dict(payloads[0][1]) if payloads else {}
        urls = [url for url, _ in payloads]
        profiles = [dict(payload.get("profile") or {}) for _, payload in payloads]
        base.update(
            {
                "kind": "pan",
                "url": (preferred if preferred in urls else (urls[0] if urls else "")),
                "urls": urls,
                "profiles": profiles,
                "season": target_season,
                "episode": episode,
                "episode_limit": episode_limit,
            }
        )
        return base

    def _cached_backend_candidates(self, resource_url):
        cache_key = str(resource_url or "").strip()
        now = time.time()
        with self._cache_lock:
            failure = self._backend_parse_failures.get(cache_key)
            if isinstance(failure, dict):
                failed_until = float(failure.get("until") or 0)
            else:
                failed_until = float(failure or 0) + self.pan_failure_cooldown
            if failure and failed_until > now:
                return True, []
            if failure:
                self._backend_parse_failures.pop(cache_key, None)
                self._backend_parse_failure_errors.pop(cache_key, None)
            cached = self._backend_parse_cache.get(cache_key)
            if not cached or now - cached[0] >= 1800:
                return False, []
            self._backend_parse_cache.move_to_end(cache_key)
            return True, list(cached[1])

    def _episode_candidates(self, candidates, target_season=None, episode_limit=None):
        episode_map = {}
        unnamed = []
        for candidate in candidates or []:
            episode = self._episode_number(candidate.get("name"), target_season)
            if episode is None:
                unnamed.append(candidate)
                continue
            if episode_limit and episode > episode_limit:
                continue
            current = episode_map.get(episode)
            if current is None or candidate["score"] > current["score"]:
                episode_map[episode] = candidate
        # Some providers return a valid ordered 1@ list but use opaque labels
        # (for example a hash or a localized title with no episode number).
        # When the site already told us the episode bound, use the backend's
        # original order only for the still-missing episode slots. This keeps
        # explicit SxxExx/01/E01 mappings authoritative and avoids treating a
        # movie-like single file as a multi-episode series.
        if unnamed and episode_limit and episode_limit <= 500:
            occupied = set(episode_map)
            unnamed.sort(key=lambda item: item.get("order", 0))
            for candidate in unnamed:
                available = next(
                    (episode for episode in range(1, episode_limit + 1) if episode not in occupied),
                    None,
                )
                if available is None:
                    break
                episode_map[available] = candidate
                occupied.add(available)
        return episode_map

    def _schedule_pan_background(self, jobs):
        pending = []
        with self._pan_background_lock:
            generation = self._pan_generation
            for provider, url in jobs:
                job_key = (generation, url)
                if job_key in self._pan_background_urls:
                    continue
                found, _ = self._cached_backend_candidates(url)
                if found:
                    continue
                self._pan_background_urls.add(job_key)
                pending.append((provider, url))
        if not pending:
            return
        worker = threading.Thread(
            target=self._pan_background_worker,
            args=(pending, generation),
            name="guanying-pan-expand",
        )
        worker.daemon = True
        worker.start()

    def _pan_background_worker(self, jobs, generation):
        if not self._pan_generation_current(generation):
            return
        workers, _ = self._pan_prewarm_limits(len(jobs))
        self._record_metric("pan_prewarm_jobs", amount=len(jobs))
        self._log("pan-background", state="start", jobs=len(jobs), workers=workers)
        try:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(
                        self._backend_parse_resource, url, "网盘", generation
                    ): (provider, url)
                    for provider, url in jobs
                }
                for future in as_completed(futures):
                    provider, url = futures[future]
                    try:
                        candidates = future.result()
                        if not self._pan_generation_current(generation):
                            continue
                        self._mark_provider_capability(provider, "enabled")
                        self._record_metric("pan_prewarm_ok")
                        self._log(
                            "pan-background",
                            state="ok",
                            provider=provider,
                            files=len(candidates),
                        )
                    except Exception as exc:
                        if not self._pan_generation_current(generation):
                            continue
                        if self._is_pan_capability_failure(exc):
                            self._mark_provider_capability(provider, "failed", exc)
                        self._record_metric("pan_prewarm_error")
                        self._log(
                            "pan-background",
                            state="error",
                            provider=provider,
                            error=type(exc).__name__,
                        )
                    finally:
                        with self._pan_background_lock:
                            self._pan_background_urls.discard((generation, url))
        finally:
            self._log("pan-background", state="done")

    def _pan_prewarm_limits(self, provider_count):
        provider_count = max(1, int(provider_count or 1))
        aggressive = bool(self.aggressive_detail_prewarm)
        with self._metrics_lock:
            stats = dict(self._pan_parse_stats)
        attempts = int(stats.get("attempts") or 0)
        successes = int(stats.get("successes") or 0)
        failure_rate = 1.0 - (float(successes) / attempts) if attempts else 0.0
        ewma_ms = float(stats.get("ewma_ms") or 0)
        if not attempts:
            workers = min(6 if aggressive else 4, max(1, (provider_count + 1) // 2))
            jobs = min(12 if aggressive else 8, max(2, provider_count + 1))
        elif failure_rate >= 0.5 or ewma_ms >= 12000:
            workers, jobs = 1, 2
        elif failure_rate >= 0.25 or ewma_ms >= 6000:
            workers, jobs = 2, 4
        elif failure_rate <= 0.1 and ewma_ms <= 2500:
            workers, jobs = (6, 12) if aggressive else (4, 8)
        else:
            workers, jobs = (4, 8) if aggressive else (3, 6)
        return (
            max(1, min(self.pan_background_workers, workers, provider_count)),
            max(2, min(self.pan_background_job_limit, jobs)),
        )

    def _record_pan_parse(self, success, elapsed_ms, failure_kind=""):
        with self._metrics_lock:
            stats = self._pan_parse_stats
            stats["attempts"] = int(stats.get("attempts") or 0) + 1
            stats["successes"] = int(stats.get("successes") or 0) + (1 if success else 0)
            previous = float(stats.get("ewma_ms") or elapsed_ms)
            stats["ewma_ms"] = previous * 0.75 + float(elapsed_ms) * 0.25
            if failure_kind:
                stats["last_failure_kind"] = failure_kind
        self._record_metric(
            "pan_parse_ok" if success else "pan_parse_error", elapsed_ms=elapsed_ms
        )

    def _backend_parse_resource(self, resource_url, resource_name, generation=None):
        if generation is not None and not self._pan_generation_current(generation):
            raise RuntimeError("后端探测已过期")
        cache_key = str(resource_url or "").strip()
        now = time.time()
        cached_candidates = None
        with self._cache_lock:
            cached = self._backend_parse_cache.get(cache_key)
            if cached and now - cached[0] < 1800:
                self._backend_parse_cache.move_to_end(cache_key)
                cached_candidates = cached[1]
        if cached_candidates is not None:
            self._record_metric("pan_parse_cache_hit")
            if cache_key.startswith(("http://", "https://")):
                self._remember_pan_profile(cache_key, cached_candidates)
            return cached_candidates

        failure_message = self._active_backend_failure(cache_key)
        if failure_message:
            self._record_metric("pan_failure_cache_hit")
            raise RuntimeError(failure_message)

        with self._cache_lock:
            inflight = self._backend_parse_inflight.get(cache_key)
            if inflight is None:
                inflight = threading.Event()
                self._backend_parse_inflight[cache_key] = inflight
                owner = True
            else:
                owner = False
        if not owner:
            wait_timeout = (
                self.pan_click_inflight_wait
                if generation is None
                else max(40, self.timeout + 10)
            )
            finished = inflight.wait(wait_timeout)
            if generation is not None and not self._pan_generation_current(generation):
                raise RuntimeError("后端探测已过期")
            with self._cache_lock:
                cached = self._backend_parse_cache.get(cache_key)
                shared_candidates = cached[1] if cached else None
                failure_message = self._backend_parse_failure_errors.get(cache_key, "")
            if shared_candidates is not None:
                if cache_key.startswith(("http://", "https://")):
                    self._remember_pan_profile(cache_key, shared_candidates)
                return shared_candidates
            if not finished:
                raise RuntimeError("后台解析仍在进行，请稍后重试")
            raise RuntimeError(failure_message or "共享后端解析失败")

        parse_started = time.perf_counter()
        try:
            session = self._backend_session()
            response = session.post(
                self._backend_endpoint("parse"),
                params={"ac": "play"},
                json={"url": cache_key},
                headers={"Content-Type": "application/json"},
                timeout=max(35, self.timeout),
            )
            data = self._response_json(response, "%s后端解析" % resource_name)
            candidates = self._backend_play_candidates(data)
            if cache_key.startswith(("http://", "https://")):
                self._remember_pan_profile(cache_key, candidates)
            if generation is not None and not self._pan_generation_current(generation):
                raise RuntimeError("后端探测已过期")
            with self._cache_lock:
                self._backend_parse_failures.pop(cache_key, None)
                self._backend_parse_failure_errors.pop(cache_key, None)
                self._backend_parse_cache[cache_key] = (time.time(), candidates)
                self._backend_parse_cache.move_to_end(cache_key)
                while len(self._backend_parse_cache) > self.MAX_CACHE:
                    self._backend_parse_cache.popitem(last=False)
            self._record_pan_parse(True, self._elapsed_ms(parse_started))
            return candidates
        except Exception as exc:
            if generation is not None and not self._pan_generation_current(generation):
                raise RuntimeError("后端探测已过期")
            if (
                generation is not None
                and cache_key.startswith(("http://", "https://"))
                and self._is_permanent_pan_link_failure(exc)
            ):
                self._remember_pan_link(cache_key, False, exc, parse_started)
            failure_kind = self._classify_pan_failure(exc)
            self._remember_backend_failure(cache_key, exc, failure_kind)
            self._record_pan_parse(False, self._elapsed_ms(parse_started), failure_kind)
            raise exc
        finally:
            with self._cache_lock:
                current = self._backend_parse_inflight.get(cache_key)
                if current is inflight:
                    self._backend_parse_inflight.pop(cache_key, None)
                    inflight.set()

    def _active_backend_failure(self, cache_key):
        now = time.time()
        with self._cache_lock:
            failure = self._backend_parse_failures.get(cache_key)
            if not failure:
                return ""
            if isinstance(failure, dict):
                failed_until = float(failure.get("until") or 0)
            else:
                failed_until = float(failure) + self.pan_failure_cooldown
            if failed_until <= now:
                self._backend_parse_failures.pop(cache_key, None)
                self._backend_parse_failure_errors.pop(cache_key, None)
                return ""
            return self._backend_parse_failure_errors.get(cache_key, "后端解析暂时不可用")

    def _remember_backend_failure(self, cache_key, error, failure_kind=None):
        kind = failure_kind or self._classify_pan_failure(error)
        now = time.time()
        with self._cache_lock:
            self._backend_parse_failures[cache_key] = {
                "at": now,
                "until": now + self._pan_failure_cooldown(kind),
                "kind": kind,
            }
            self._backend_parse_failure_errors[cache_key] = self._short_backend_error(error)
            while len(self._backend_parse_failures) > self.MAX_CACHE:
                oldest = next(iter(self._backend_parse_failures))
                self._backend_parse_failures.pop(oldest, None)
                self._backend_parse_failure_errors.pop(oldest, None)

    def _season_number(self, title):
        text = self._clean_text(title)
        match = re.search(r"(?:第\s*)?(\d{1,2})\s*季|Season\s*(\d{1,2})", text, re.I)
        if match:
            return self._safe_int(match.group(1) or match.group(2)) or None
        chinese = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6,
                   "七": 7, "八": 8, "九": 9, "十": 10}
        match = re.search(r"第\s*([一二三四五六七八九十])\s*季", text)
        return chinese.get(match.group(1)) if match else None

    def _episode_number(self, name, target_season=None):
        text = self._clean_text(name)
        season_match = re.search(r"S\s*0*(\d{1,2})\s*E(?:P)?\s*0*(\d{1,3})", text, re.I)
        if season_match:
            season = self._safe_int(season_match.group(1))
            episode = self._safe_int(season_match.group(2))
            if target_season and season != target_season:
                return None
            return episode if 0 < episode <= 500 else None
        patterns = (
            r"^\s*0*(\d{1,3})(?=[\s._-])",
            r"(?:^|[^A-Z])EP?\s*0*(\d{1,3})(?:[^0-9]|$)",
            r"第\s*0*(\d{1,3})\s*[集话期]",
            r"\[\s*0*(\d{1,3})\s*\]",
        )
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                episode = self._safe_int(match.group(1))
                if 0 < episode <= 500:
                    return episode
        if self.VIDEO_EXT_RE.search(text):
            match = re.search(
                r"(?:^|[\s._-])0*(\d{1,3})(?=[\s._-]|\.[A-Za-z0-9]{2,5}$)",
                text,
                re.I,
            )
            if match:
                episode = self._safe_int(match.group(1))
                if 0 < episode <= 500:
                    return episode
        return None

    def _quality_label(self, name):
        text = str(name or "")
        labels = []
        if re.search(r"2160|4K|UHD", text, re.I):
            labels.append("4K")
        elif re.search(r"1080", text, re.I):
            labels.append("1080P")
        elif re.search(r"720", text, re.I):
            labels.append("720P")
        if re.search(r"高码(?:率)?|REMUX|BluRay|BDREMUX|WEB[- .]?DL", text, re.I):
            labels.append("高码")
        if re.search(r"Dolby\s*Vision|杜比视界|(?:^|[ ._-])DV(?:[ ._-]|$)|dvhe", text, re.I):
            labels.append("DV")
        elif re.search(r"HDR", text, re.I):
            labels.append("HDR")
        if re.search(r"H\.?264|AVC|x264", text, re.I):
            labels.append("AVC")
        return "·".join(labels)

    def _extract_online(self, playlist, dir_name, item_id):
        if not isinstance(playlist, list):
            return []
        entries = []
        seen = set()
        for source_index, source in enumerate(playlist):
            if not isinstance(source, dict):
                continue
            source_id = str(source.get("i") or source.get("id") or "").strip()
            source_name = self._clean_text(source.get("t") or source.get("name") or "在线播放")
            episodes = source.get("list") or source.get("episodes") or []
            if not source_id or not isinstance(episodes, list):
                continue
            expanded = self._expand_online_episodes(episodes)
            for episode_index, episode_name in expanded:
                key = episode_index
                if key in seen:
                    continue
                seen.add(key)
                label = self._safe_label("第%s集" % episode_index)
                payload = {
                    "kind": "online",
                    "dir": dir_name,
                    "id": item_id,
                    "source": source_id,
                    "episode": episode_index,
                }
                entries.append(
                    {
                        "label": label,
                        "id": self._pack(payload),
                        "source": source_name,
                    }
                )
        if not entries:
            return []
        return [("采集源", [(item["label"], item["id"]) for item in entries])]

    def _expand_online_episodes(self, episodes):
        """Expand the site's compact playlist ranges into (episode, label)."""
        result = []
        if not isinstance(episodes, list):
            return result
        for index, raw in enumerate(episodes, start=1):
            if isinstance(raw, str):
                label = self._clean_text(raw) or "第%s集" % index
                match = re.search(r"(?:第\s*)?(\d+)", label)
                result.append((int(match.group(1)) if match else index, label))
                continue
            if isinstance(raw, (tuple, list)) and len(raw) == 2:
                template, value = raw
                if isinstance(template, (tuple, list)) and template:
                    parts = [self._clean_text(part) for part in template]
                    if isinstance(value, (tuple, list)) and len(value) >= 2:
                        start, end = self._safe_int(value[0]), self._safe_int(value[1])
                    else:
                        start = end = self._safe_int(value)
                    if start > 0 and end >= start and end - start <= 500:
                        prefix = parts[0] if parts else ""
                        suffix = parts[1] if len(parts) > 1 else ""
                        for episode in range(start, end + 1):
                            result.append((episode, "%s%s%s" % (prefix, episode, suffix)))
                        continue
            if isinstance(raw, (tuple, list)):
                nested = self._expand_online_episodes(list(raw))
                if nested:
                    result.extend(nested)
                    continue
            label = self._clean_text(raw) or "第%s集" % index
            result.append((index, label))
        return result

    def _build_detail_vod(self, metadata, groups, stats, dir_name, item_id):
        play_from = []
        play_url = []
        vod_flags = []
        prompt_label = "选集播放"
        prompt_id = self._status_id("请选择具体集数")
        for group_name, items in groups:
            valid = []
            for label, value in items:
                payload = self._unpack(str(value or ""))
                if not payload or payload.get("kind") not in (
                    "pan",
                    "backend",
                    "online",
                    "magnet",
                ):
                    continue
                valid.append((self._safe_label(label), value))
            if not valid:
                continue
            safe_group = self._safe_label(group_name)
            prompted = [(prompt_label, prompt_id)] + valid
            urls = "#".join("%s$%s" % item for item in prompted)
            play_from.append(safe_group)
            play_url.append(urls)
            vod_flags.append(
                {
                    "flag": safe_group,
                    "urls": urls,
                    "position": 0,
                    "episodes": [
                        {"name": prompt_label, "url": prompt_id, "selected": True}
                    ]
                    + [
                        {"name": label, "url": value, "selected": False}
                        for label, value in valid
                    ],
                }
            )
        diag = "资源分组%s组：磁力%s，网盘%s，在线%s。当前镜像：%s" % (
            stats.get("groups", 0),
            stats.get("magnet", 0),
            stats.get("pan", 0),
            stats.get("online", 0),
            self._host or "未选择",
        )
        if stats.get("pan_total"):
            diag += "。网盘分集已完成%s/%s，后台处理中%s" % (
                stats.get("pan_ready", 0),
                stats.get("pan_total", 0),
                stats.get("pan_pending", 0),
            )
        content = metadata.get("content") or ""
        if content:
            content += "\n\n"
        content += diag
        return {
            "vod_id": self._vod_id(dir_name, item_id),
            "vod_name": metadata.get("title") or "未知影片",
            "vod_pic": metadata.get("pic") or self._image(dir_name, item_id, 384),
            "type_name": metadata.get("genres") or "",
            "vod_year": metadata.get("year") or "",
            "vod_area": metadata.get("areas") or "",
            "vod_remarks": metadata.get("remarks") or diag,
            "vod_actor": metadata.get("actors") or "",
            "vod_director": metadata.get("directors") or "",
            "vod_content": content[:1800],
            "vod_play_from": "$$$".join(play_from),
            "vod_play_url": "$$$".join(play_url),
            "vodFlags": vod_flags,
        }

    def _play_pan(self, payload):
        click_started = time.perf_counter()
        url = str(payload.get("url") or "").strip()
        urls = payload.get("urls") or []
        if isinstance(urls, str):
            urls = [urls]
        candidates = []
        for candidate in [url] + list(urls):
            candidate = str(candidate or "").strip()
            if candidate.startswith(("http://", "https://")) and candidate not in candidates:
                candidates.append(candidate)
            if len(candidates) >= self.pan_fallback_links:
                break
        if not candidates:
            self._record_metric("pan_click_error", elapsed_ms=self._elapsed_ms(click_started))
            return self._player_error("网盘地址无效")
        candidates, cooled = self._rank_pan_links(
            candidates,
            self._safe_int(payload.get("episode_limit")) or None,
            self._safe_int(payload.get("episode")) or None,
            payload.get("profiles") or [payload.get("profile") or {}],
        )
        if not candidates:
            message = cooled[0].get("error") if cooled else "候选分享处于冷却"
            permanent = cooled and all(
                item.get("failure_kind") == "permanent" for item in cooled
            )
            prefix = "候选网盘分享均已失效" if permanent else "候选网盘分享暂不可用"
            self._record_metric("pan_click_error", elapsed_ms=self._elapsed_ms(click_started))
            return self._player_error("%s: %s" % (prefix, message))
        failures = []
        for candidate in candidates:
            started = time.perf_counter()
            try:
                result = self._play_backend_resource(
                    candidate,
                    "网盘",
                    target_season=self._safe_int(payload.get("season")) or None,
                    target_episode=self._safe_int(payload.get("episode")) or None,
                    episode_limit=self._safe_int(payload.get("episode_limit")) or None,
                )
                self._remember_pan_link(candidate, True, "", started)
                self._mark_provider_capability(payload.get("provider"), "enabled")
                self._record_metric("pan_click_ok", elapsed_ms=self._elapsed_ms(click_started))
                return result
            except Exception as exc:
                self._remember_pan_link(candidate, False, exc, started)
                failures.append((candidate, exc))
                self._diag("backend_pan", error=str(exc), url=candidate)
                self._log(
                    "pan-play-fallback",
                    provider=payload.get("provider") or "unknown",
                    attempt=len(failures),
                    error=self._short_backend_error(exc),
                )
        capability_error = next(
            (exc for _, exc in failures if self._is_pan_capability_failure(exc)), None
        )
        if capability_error is not None:
            self._mark_provider_capability(payload.get("provider"), "failed", capability_error)
        push_candidate = next(
            (candidate for candidate, exc in failures if not self._is_permanent_pan_link_failure(exc)),
            "",
        )
        message = self._short_backend_error(failures[0][1]) if failures else "网盘解析失败"
        self._record_metric("pan_click_error", elapsed_ms=self._elapsed_ms(click_started))
        if push_candidate:
            return {
                "parse": 0,
                "jx": 0,
                "playUrl": "",
                "url": "push://" + push_candidate,
                "header": {},
                "msg": "候选网盘均未解析成功，已回退推送: %s" % message,
            }
        return self._player_error("候选网盘分享均已失效: %s" % message)

    def _rank_pan_links(self, urls, episode_limit=None, target_episode=None, profiles=None):
        now = time.time()
        parse_ready = {}
        for url in urls:
            found, candidates = self._cached_backend_candidates(url)
            parse_ready[url] = bool(found and candidates)
        static_profiles = {
            str(item.get("url") or "").strip(): dict(item)
            for item in (profiles or [])
            if isinstance(item, dict) and item.get("url")
        }
        with self._cache_lock:
            history = {url: dict(self._pan_link_history.get(url) or {}) for url in urls}
        active = []
        cooled = []
        for order, url in enumerate(urls):
            record = history.get(url) or {}
            item = {"url": url, "order": order, **(static_profiles.get(url) or {}), **record}
            if float(record.get("failed_until") or 0) > now:
                cooled.append(item)
            else:
                active.append(item)

        def score(item):
            attempts = int(item.get("attempts") or 0)
            successes = int(item.get("successes") or 0)
            success_rate = float(successes) / attempts if attempts else 0.5
            profiled_episodes = int(item.get("episode_count") or 0)
            episode_numbers = {
                self._safe_int(value) for value in item.get("episode_numbers") or []
            }
            if target_episode and episode_numbers:
                target_match = 2 if target_episode in episode_numbers else 0
            else:
                target_match = 1
            if episode_limit and profiled_episodes:
                completeness = min(1.0, float(profiled_episodes) / episode_limit)
            else:
                completeness = 0.5
            availability = 2 if successes else (1 if not attempts else 0)
            return (
                target_match,
                1 if parse_ready.get(item["url"]) else 0,
                availability,
                completeness,
                int(item.get("quality") or 0),
                1 if item.get("high_bitrate") else 0,
                int(item.get("max_size") or 0),
                success_rate,
                -int(item.get("average_ms") or 999999),
                -int(item.get("order") or 0),
            )

        active.sort(key=score, reverse=True)
        return [item["url"] for item in active], cooled

    def _pan_link_available(self, url):
        with self._cache_lock:
            record = dict(self._pan_link_history.get(str(url or "").strip()) or {})
        return float(record.get("failed_until") or 0) <= time.time()

    def _remember_pan_link(self, url, success, error, started):
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        failure_kind = "" if success else self._classify_pan_failure(error)
        with self._cache_lock:
            current = dict(self._pan_link_history.get(url) or {})
            attempts = int(current.get("attempts") or 0) + 1
            successes = int(current.get("successes") or 0) + (1 if success else 0)
            previous_ms = int(current.get("average_ms") or elapsed_ms)
            current.update(
                {
                    "attempts": attempts,
                    "successes": successes,
                    "average_ms": int(previous_ms * 0.7 + elapsed_ms * 0.3),
                    "error": "" if success else self._short_backend_error(error),
                    "failure_kind": failure_kind,
                    "failed_until": (
                        time.time() + self._pan_failure_cooldown(failure_kind)
                        if not success
                        else 0
                    ),
                }
            )
            self._pan_link_history[url] = current
            self._pan_link_history.move_to_end(url)
            while len(self._pan_link_history) > self.MAX_CACHE:
                self._pan_link_history.popitem(last=False)

    def _remember_pan_profile(self, url, candidates):
        episode_numbers = {
            episode
            for episode in (self._episode_number(item.get("name")) for item in candidates or [])
            if episode is not None
        }
        names = [str(item.get("name") or "") for item in candidates or []]
        likely_file_count = sum(
            1 for name in names if not re.search(r"广告|推广|sample|预告|片头|片尾|花絮", name, re.I)
        )
        with self._cache_lock:
            current = dict(self._pan_link_history.get(url) or {})
            current.update(
                {
                    "episode_count": len(episode_numbers) or likely_file_count,
                    "episode_numbers": sorted(episode_numbers),
                    "file_count": len(candidates or []),
                    "quality": max((self._quality_score(name) for name in names), default=0),
                    "high_bitrate": any(
                        re.search(r"高码(?:率)?|REMUX|BluRay|BDREMUX|WEB[- .]?DL", name, re.I)
                        for name in names
                    ),
                    "max_size": max((self._size_bytes(name) for name in names), default=0),
                    "profiled_at": time.time(),
                }
            )
            self._pan_link_history[url] = current
            self._pan_link_history.move_to_end(url)
            while len(self._pan_link_history) > self.MAX_CACHE:
                self._pan_link_history.popitem(last=False)

    def _is_permanent_pan_link_failure(self, error):
        return bool(
            re.search(
                r"分享者.*封禁|链接.*受限|分享.*失效|链接.*失效|已取消分享|不存在|已过期|被删除",
                self._clean_text(str(error or "")),
                re.I,
            )
        )

    def _is_pan_capability_failure(self, error):
        return bool(
            re.search(
                r"找不到.*(?:帐号|账号)|未配置.*(?:帐号|账号|网盘)|(?:帐号|账号).*不可用|接口.*不可用",
                self._clean_text(str(error or "")),
                re.I,
            )
        )

    def _classify_pan_failure(self, error):
        text = self._clean_text(str(error or ""))
        if self._is_pan_capability_failure(text):
            return "capability"
        if self._is_permanent_pan_link_failure(text):
            return "permanent"
        if re.search(r"(?:HTTP\s*)?429|请求.*频繁|访问.*频繁|限流|频率.*限制|too many", text, re.I):
            return "rate_limit"
        if re.search(
            r"timeout|timed out|连接|network|temporary|HTTP\s*(?:502|503|504)|"
            r"稍后重试|处理中|未在.*完成|后端探测已过期",
            text,
            re.I,
        ):
            return "transient"
        return "unknown"

    def _pan_failure_cooldown(self, failure_kind):
        return {
            "capability": self.pan_capability_failure_cooldown,
            "permanent": self.pan_link_failure_cooldown,
            "rate_limit": self.pan_rate_limit_cooldown,
            "transient": self.pan_transient_failure_cooldown,
            "unknown": self.pan_unknown_failure_cooldown,
        }.get(str(failure_kind or "unknown"), self.pan_unknown_failure_cooldown)

    def _play_online(self, payload):
        click_started = time.perf_counter()
        source_id = str(payload.get("source") or "").strip()
        episode = self._page_number(payload.get("episode"))
        if not source_id:
            return self._player_error("在线线路 ID 为空")
        dir_name = str(payload.get("dir") or "")
        item_id = str(payload.get("id") or "")
        cache_key = (dir_name, item_id, episode)
        cached = self._cached_online_result(cache_key)
        if cached:
            self._record_metric("online_click_cache", elapsed_ms=self._elapsed_ms(click_started))
            return self._online_player_result(cached)
        candidates = []
        try:
            resources = self._request_resources(dir_name, item_id)
            for order, source in enumerate(resources.get("playlist") or []):
                alt_id = str(source.get("i") or source.get("id") or "").strip()
                episodes = source.get("list") or []
                available = {number for number, _ in self._expand_online_episodes(episodes)}
                if alt_id and episode in available:
                    candidates.append(
                        {
                            "id": alt_id,
                            "name": self._clean_text(
                                source.get("t") or source.get("name") or "线路%s" % (order + 1)
                            ),
                            "order": order,
                            "episode_count": len(available),
                        }
                    )
        except Exception:
            pass
        if not candidates:
            candidates = [{"id": source_id, "name": "默认线路", "order": 0, "episode_count": 0}]
        elif source_id and all(item["id"] != source_id for item in candidates):
            candidates.insert(0, {"id": source_id, "name": "默认线路", "order": -1, "episode_count": 0})

        ranked = self._rank_online_candidates(candidates)
        selected, completed = self._race_online_candidates(ranked, episode)
        if selected:
            self._remember_online_result(cache_key, selected)
            self._record_metric("online_click_ok", elapsed_ms=self._elapsed_ms(click_started))
            self._log(
                "online-select",
                source=selected.get("name") or selected.get("id"),
                episode=episode,
                height=selected.get("height", 0),
                bandwidth=selected.get("bandwidth", 0),
                elapsed_ms=selected.get("total_ms", 0),
                tested=len(completed),
            )
            return self._online_player_result(selected)

        errors = [item.get("error") for item in completed if item.get("error")]
        attempted = {item.get("id") for item in ranked[: self.online_probe_workers]}
        for candidate in ranked:
            if candidate["id"] in attempted:
                continue
            try:
                source, _ = self._request_text("/py/%s/%s" % (candidate["id"], episode))
                data = self._extract_obj(source, "player")
                url = str((data or {}).get("url") or "").strip()
                if url.startswith(("http://", "https://")):
                    fallback = dict(candidate, url=url, media_ok=False, resolve_ok=True)
                    self._remember_online_result(cache_key, fallback)
                    self._record_metric("online_click_ok", elapsed_ms=self._elapsed_ms(click_started))
                    return self._online_player_result(fallback)
            except Exception as exc:
                errors.append(str(exc))
        self._record_metric("online_click_error", elapsed_ms=self._elapsed_ms(click_started))
        return self._player_error("在线线路均解析失败%s" % ((": " + errors[0]) if errors else ""))

    def _online_executor_instance(self):
        with self._online_executor_lock:
            if self._online_executor is None:
                self._online_executor = ThreadPoolExecutor(max_workers=self.online_probe_workers)
            return self._online_executor

    def _race_online_candidates(self, candidates, episode):
        selected_candidates = candidates[: self.online_probe_workers]
        if not selected_candidates:
            return None, []
        try:
            host, shared_session = self._client()
            cookies = shared_session.cookies.get_dict()
        except Exception:
            return None, []
        executor = self._online_executor_instance()
        pending = {
            executor.submit(self._probe_online_candidate, item, episode, host, cookies)
            for item in selected_candidates
        }
        completed = []
        deadline = time.perf_counter() + self.online_probe_deadline_ms / 1000.0
        first_success_deadline = None
        while pending:
            active_deadline = deadline
            if first_success_deadline is not None:
                active_deadline = min(active_deadline, first_success_deadline)
            remaining = active_deadline - time.perf_counter()
            if remaining <= 0:
                break
            done, pending = wait(pending, timeout=remaining, return_when=FIRST_COMPLETED)
            if not done:
                break
            for future in done:
                try:
                    result = future.result()
                except Exception as exc:
                    result = {"media_ok": False, "resolve_ok": False, "error": str(exc)}
                completed.append(result)
                if result.get("media_ok") and first_success_deadline is None:
                    first_success_deadline = (
                        time.perf_counter() + self.online_quality_grace_ms / 1000.0
                    )
        playable = [item for item in completed if item.get("media_ok")]
        if not playable:
            playable = [item for item in completed if item.get("resolve_ok")]
        return (max(playable, key=self._online_result_score) if playable else None), completed

    def _probe_online_candidate(self, candidate, episode, host, cookies):
        result = dict(candidate)
        result.update(
            {
                "resolve_ok": False,
                "media_ok": False,
                "resolve_ms": 0,
                "media_ms": 0,
                "total_ms": 0,
                "height": 0,
                "bandwidth": 0,
                "url": "",
                "error": "",
            }
        )
        started = time.perf_counter()
        session = self._new_session()
        session.cookies.update(cookies or {})
        try:
            response = session.get(
                "%s/py/%s/%s" % (host, quote(candidate["id"]), episode),
                headers={"Referer": host + "/"},
                timeout=min(self.timeout, 8),
            )
            response.raise_for_status()
            result["resolve_ms"] = int((time.perf_counter() - started) * 1000)
            data = self._extract_obj(response.text, "player") or {}
            media_url = str(data.get("url") or "").strip()
            if not media_url.startswith(("http://", "https://")):
                raise RuntimeError("播放页没有返回HTTP媒体地址")
            result["resolve_ok"] = True
            result["url"] = media_url
            media_started = time.perf_counter()
            media = session.get(
                media_url,
                headers={
                    "Referer": host + "/",
                    "Range": "bytes=0-%s" % (self.online_probe_bytes - 1),
                },
                timeout=min(self.timeout, 6),
                stream=True,
            )
            payload = media.raw.read(self.online_probe_bytes, decode_content=False)
            result["media_ms"] = int((time.perf_counter() - media_started) * 1000)
            content_type = str(media.headers.get("Content-Type") or "").lower()
            text = payload.decode("utf-8", errors="ignore")
            is_html = "text/html" in content_type or text.lstrip().lower().startswith("<!doctype html")
            result["media_ok"] = media.status_code in (200, 206) and bool(payload) and not is_html
            result.update(self._online_media_quality(media_url, text))
            media.close()
        except Exception as exc:
            result["error"] = "%s: %s" % (type(exc).__name__, str(exc)[:180])
        finally:
            result["total_ms"] = int((time.perf_counter() - started) * 1000)
            session.close()
            self._remember_online_history(result)
        return result

    def _online_media_quality(self, url, text):
        heights = [int(value) for value in re.findall(r"RESOLUTION=\d+x(\d+)", text, re.I)]
        bandwidths = [int(value) for value in re.findall(r"BANDWIDTH=(\d+)", text, re.I)]
        lowered = str(url or "").lower()
        inferred = 0
        for marker, height in (("2160", 2160), ("4k", 2160), ("1080", 1080), ("720", 720), ("480", 480)):
            if marker in lowered:
                inferred = max(inferred, height)
        return {
            "height": max(heights + [inferred]) if heights or inferred else 0,
            "bandwidth": max(bandwidths) if bandwidths else 0,
        }

    def _online_line_key(self, name):
        return re.sub(r"\s*\([^)]*\)\s*$", "", self._clean_text(name)).lower()

    def _remember_online_history(self, result):
        key = self._online_line_key(result.get("name") or result.get("id"))
        if not key:
            return
        with self._cache_lock:
            current = dict(self._online_history.get(key) or {})
            attempts = int(current.get("attempts") or 0) + 1
            successes = int(current.get("successes") or 0) + (1 if result.get("media_ok") else 0)
            previous_ms = int(current.get("average_ms") or result.get("total_ms") or 0)
            current.update(
                {
                    "attempts": attempts,
                    "successes": successes,
                    "average_ms": int(previous_ms * 0.7 + int(result.get("total_ms") or 0) * 0.3),
                    "height": max(int(current.get("height") or 0), int(result.get("height") or 0)),
                    "bandwidth": max(int(current.get("bandwidth") or 0), int(result.get("bandwidth") or 0)),
                    "updated": time.time(),
                }
            )
            self._online_history[key] = current
            self._online_history.move_to_end(key)
            while len(self._online_history) > self.MAX_CACHE:
                self._online_history.popitem(last=False)

    def _rank_online_candidates(self, candidates):
        with self._cache_lock:
            history = {key: dict(value) for key, value in self._online_history.items()}

        def score(item):
            record = history.get(self._online_line_key(item.get("name") or item.get("id"))) or {}
            attempts = int(record.get("attempts") or 0)
            success_rate = (float(record.get("successes") or 0) / attempts) if attempts else -1.0
            return (
                1 if int(record.get("successes") or 0) else 0,
                success_rate if attempts else 0.5,
                int(record.get("height") or 0),
                int(record.get("bandwidth") or 0),
                -int(record.get("average_ms") or 999999),
                int(item.get("episode_count") or 0),
                -int(item.get("order") or 0),
            )

        return sorted(candidates, key=score, reverse=True)

    def _online_result_score(self, item):
        key = self._online_line_key(item.get("name") or item.get("id"))
        with self._cache_lock:
            record = dict(self._online_history.get(key) or {})
        attempts = int(record.get("attempts") or 0)
        success_rate = (float(record.get("successes") or 0) / attempts) if attempts else 0.0
        return (
            1 if item.get("media_ok") else 0,
            int(item.get("height") or 0),
            int(item.get("bandwidth") or 0),
            success_rate,
            -int(item.get("total_ms") or 999999),
            -int(item.get("order") or 0),
        )

    def _online_player_result(self, item):
        return {
            "parse": 0,
            "jx": 0,
            "playUrl": "",
            "url": str(item.get("url") or ""),
            "header": {
                "User-Agent": self.headers["User-Agent"],
                "Referer": (self._host or self.address_page) + "/",
            },
        }

    def _cached_online_result(self, key):
        with self._cache_lock:
            cached = self._online_result_cache.get(key)
            if not cached or time.time() - cached[0] > self.online_result_ttl:
                if cached:
                    self._online_result_cache.pop(key, None)
                return None
            self._online_result_cache.move_to_end(key)
            return dict(cached[1])

    def _remember_online_result(self, key, result):
        with self._cache_lock:
            self._online_result_cache[key] = (time.time(), dict(result))
            self._online_result_cache.move_to_end(key)
            while len(self._online_result_cache) > self.MAX_CACHE:
                self._online_result_cache.popitem(last=False)

    def _play_magnet(self, payload):
        magnet = self._normalize_magnet(payload.get("magnet"), payload.get("title"))
        magnets = payload.get("magnets") or []
        if isinstance(magnets, str):
            magnets = [magnets]
        candidates = []
        for candidate in [magnet] + list(magnets):
            normalized = self._normalize_magnet(candidate, payload.get("title"))
            if normalized and normalized not in candidates:
                candidates.append(normalized)
            if len(candidates) >= self.magnet_fallback_links:
                break
        if not candidates:
            return self._player_error("磁力地址无效")
        errors = []
        for candidate in candidates:
            try:
                return self._play_backend_resource(
                    candidate,
                    "磁力",
                    target_season=self._safe_int(payload.get("season")) or None,
                    target_episode=self._safe_int(payload.get("episode")) or None,
                    episode_limit=self._safe_int(payload.get("episode_limit")) or None,
                )
            except Exception as exc:
                errors.append((candidate, exc))
                message = self._short_backend_error(exc)
                if re.search(r"未在.*完成|稍后重试|处理中|timeout", message, re.I):
                    break
        if errors:
            magnet, exc = errors[-1]
            message = self._short_backend_error(exc)
            if re.search(r"未在.*完成|稍后重试|处理中|timeout", message, re.I):
                message = "离线下载中，稍后重试: " + message
            if self.magnet_raw_fallback:
                self._log("magnet-fallback", reason=message[:120])
                return {
                    "parse": 0,
                    "jx": 0,
                    "playUrl": "",
                    "url": magnet,
                    "header": {},
                    "msg": "后端磁力解析失败，已回退原始磁力地址: %s" % message,
                }
            return self._player_error(message)
        return self._player_error("磁力解析失败")

    def _short_backend_error(self, error):
        text = self._clean_text(str(error or ""))
        if not text:
            return "后端解析失败"
        if "115接口返回非JSON响应" in text or "115科技官网" in text:
            return "115接口返回网页，当前115账号或接口不可用"
        if len(text) > 240:
            text = text[:237] + "..."
        return text

    def _play_backend_resource(
        self,
        resource_url,
        resource_name,
        target_season=None,
        target_episode=None,
        episode_limit=None,
    ):
        if not self.alist_api or not self.alist_token:
            raise RuntimeError("未配置原版 AList-TVBox 后端地址或 Token")

        candidates = self._backend_parse_resource(resource_url, resource_name)
        if not candidates:
            raise RuntimeError("%s后端解析未返回 1@ 播放项" % resource_name)

        if target_season or target_episode:
            episode_map = self._episode_candidates(
                candidates, target_season, episode_limit
            )
            if episode_map:
                if target_episode:
                    selected = episode_map.get(target_episode)
                    if selected is None:
                        raise RuntimeError("%s未找到第%s集" % (resource_name, target_episode))
                    candidates = [selected]
                else:
                    candidates = [episode_map[min(episode_map)]]
            elif target_episode:
                raise RuntimeError("%s未识别出第%s集" % (resource_name, target_episode))

        errors = []
        for candidate in candidates:
            try:
                return self._backend_play(candidate["id"])
            except Exception as exc:
                errors.append(str(exc))
        raise RuntimeError(errors[0] if errors else "%s后端播放失败" % resource_name)

    def _backend_endpoint(self, name):
        token = quote(str(self.alist_token or "").strip(), safe="")
        suffix = "/%s" % token if token else ""
        return "%s/%s%s" % (self.alist_api.rstrip("/"), name, suffix)

    def _response_json(self, response, action):
        try:
            data = response.json() if response.content else {}
        except Exception:
            data = {}
        if response.status_code >= 400:
            message = str(
                data.get("detail")
                or data.get("msg")
                or data.get("error")
                or "%s HTTP %s" % (action, response.status_code)
            )
            raise RuntimeError(message)
        if not isinstance(data, dict):
            raise RuntimeError("%s返回结构异常" % action)
        return data

    def _backend_play_candidates(self, data):
        candidates = []
        seen = set()
        rows = data.get("list") or data.get("data") or data.get("result") or []
        if isinstance(rows, dict):
            nested = rows.get("list")
            rows = nested if isinstance(nested, list) else [rows]
        serial = 0
        for vod in rows:
            if not isinstance(vod, dict):
                continue
            from_groups = str(
                vod.get("vod_play_from") or vod.get("play_from") or vod.get("from") or ""
            ).split("$$$")
            url_groups = str(
                vod.get("vod_play_url") or vod.get("play_url") or vod.get("url") or ""
            ).split("$$$")
            for group_index, url_group in enumerate(url_groups):
                play_from = from_groups[group_index] if group_index < len(from_groups) else ""
                for episode_index, episode in enumerate(str(url_group or "").split("#")):
                    serial += 1
                    label, separator, target = str(episode or "").partition("$")
                    play_id = str(target if separator else label).strip()
                    if not play_id.startswith("1@") or play_id in seen:
                        continue
                    seen.add(play_id)
                    name = self._clean_text(label) or "%s-%s" % (play_from, episode_index + 1)
                    candidates.append(
                        {
                            "id": play_id,
                            "name": name,
                            "score": self._backend_candidate_score(name, episode_index),
                            "order": serial,
                        }
                    )
        candidates.sort(key=lambda item: item["score"], reverse=True)
        return candidates

    def _backend_candidate_score(self, name, order):
        text = str(name or "")
        negative = bool(re.search(r"广告|推广|sample|预告|片头|片尾|花絮", text, re.I))
        subtitle = bool(self.SUBTITLE_RE.search(text))
        high_bitrate = bool(
            re.search(r"高码(?:率)?|REMUX|BluRay|BDREMUX|WEB[- .]?DL", text, re.I)
        )
        return (
            0 if negative else 1,
            1 if high_bitrate else 0,
            self._size_bytes(text),
            self._quality_score(text),
            1 if subtitle else 0,
            -order,
        )

    def _backend_play(self, play_id):
        cache_key = str(play_id or "").strip()
        now = time.time()
        with self._cache_lock:
            cached = self._backend_play_cache.get(cache_key)
            if cached and now - cached[0] < self.backend_play_cache_ttl:
                self._backend_play_cache.move_to_end(cache_key)
                result = dict(cached[1])
                if isinstance(result.get("header"), dict):
                    result["header"] = dict(result["header"])
                self._record_metric("backend_play_cache_hit")
                return result
            if cached:
                self._backend_play_cache.pop(cache_key, None)
        session = self._backend_session()
        response = session.get(
            self._backend_endpoint("play"),
            params={"id": play_id, "type": "client-proxy", "from": "jar"},
            timeout=max(25, self.timeout),
        )
        data = self._response_json(response, "后端播放")
        url = data.get("url")
        if isinstance(url, str) and url.startswith("/"):
            data["url"] = self.alist_api.rstrip("/") + url
            url = data["url"]
        if not url:
            raise RuntimeError(str(data.get("msg") or data.get("error") or "后端播放地址为空"))
        data["parse"] = 0
        data["jx"] = 0
        data.setdefault("playUrl", "")
        data.setdefault("header", {})
        with self._cache_lock:
            self._backend_play_cache[cache_key] = (time.time(), dict(data))
            self._backend_play_cache.move_to_end(cache_key)
            while len(self._backend_play_cache) > self.MAX_CACHE:
                self._backend_play_cache.popitem(last=False)
        return data

    def _validate_resource_payload(self, data):
        downlist = data.get("downlist") or {}
        listing = downlist.get("list") if isinstance(downlist, dict) else {}
        if listing and not isinstance(listing, dict):
            raise RuntimeError("磁力列表结构异常")
        panlist = data.get("panlist") or {}
        if panlist and not isinstance(panlist, dict):
            raise RuntimeError("网盘列表结构异常")
        playlist = data.get("playlist") or []
        if playlist and not isinstance(playlist, list):
            raise RuntimeError("在线播放结构异常")

    def _extract_all_objs(self, source):
        result = {}
        decoder = json.JSONDecoder()
        for match in re.finditer(r"_obj\.([A-Za-z0-9_]+)\s*=", str(source or "")):
            key = match.group(1)
            start = match.end()
            while start < len(source) and source[start].isspace():
                start += 1
            try:
                value, _ = decoder.raw_decode(source[start:])
                result[key] = value
            except Exception:
                continue
        return result

    def _extract_obj(self, source, key):
        return self._extract_all_objs(source).get(key)

    def _normalize_magnet(self, value, title=""):
        raw = html_lib.unescape(str(value or "")).strip()
        if raw and not raw.lower().startswith("magnet:?"):
            if re.fullmatch(r"[A-Fa-f0-9]{40}|[A-Z2-7]{32}", raw, re.I):
                raw = "magnet:?xt=urn:btih:" + raw
            else:
                match = self.MAGNET_RE.search(raw)
                raw = match.group(0) if match else ""
        match = self.BTIH_RE.search(raw)
        if not match:
            return ""
        info_hash = match.group(1).lower()
        extras = []
        if "?" in raw:
            for key, item in parse_qsl(raw.split("?", 1)[1], keep_blank_values=True):
                if key.lower() in ("dn", "tr", "xl", "ws", "xs", "as", "kt"):
                    extras.append((key, item))
                if len(extras) >= 16:
                    break
        if title and not any(key.lower() == "dn" for key, _ in extras):
            extras.insert(0, ("dn", self._clean_text(title)))
        result = "magnet:?xt=urn:btih:" + info_hash
        if extras:
            result += "&" + urlencode(extras, doseq=True)
        return result

    def _configure_pan_capabilities(self, config):
        self._pan_capability_configured = False
        self._pan_capability = {}
        raw = config.get("local_proxy_config") or config.get("local_proxy")
        if not raw and isinstance(config.get("data"), dict):
            raw = config["data"].get("local_proxy_config") or config["data"].get("local_proxy")
        if isinstance(raw, str):
            text = raw.strip()
            if text:
                try:
                    raw = json.loads(text)
                except Exception:
                    try:
                        raw = ast.literal_eval(text)
                    except Exception:
                        raw = None
        if not isinstance(raw, dict):
            self._log("pan-capability-init", mode="probe", configured=0)
            return
        self._pan_capability_configured = True
        capabilities = {}
        for key, value in raw.items():
            name = str(key or "").strip().upper()
            if isinstance(value, dict):
                enabled = self._bool_value(value.get("enabled"), False)
            else:
                enabled = self._bool_value(value, False)
            configured_state = "enabled" if enabled else "disabled"
            capabilities[name] = {
                "state": configured_state,
                "configured_state": configured_state,
                "error": "",
                "failed_until": 0,
            }
        self._pan_capability = capabilities
        self._log(
            "pan-capability-init",
            mode="config",
            configured=len(capabilities),
            enabled=sum(1 for item in capabilities.values() if item.get("state") == "enabled"),
        )

    def _pan_generation_current(self, generation):
        with self._pan_background_lock:
            return generation == self._pan_generation

    def _provider_accel_key(self, provider):
        name = str(provider or "").strip()
        return self.PROVIDER_ACCEL_KEYS.get(name) or ("PROVIDER:" + name if name else "")

    def _provider_allowed(self, provider):
        key = self._provider_accel_key(provider)
        entry = dict(self._pan_capability.get(key) or {})
        if entry.get("state") == "failed":
            failed_until = float(entry.get("failed_until") or 0)
            if failed_until and time.time() >= failed_until:
                restored = entry.get("configured_state") or "unknown"
                entry.update({"state": restored, "error": "", "failed_until": 0})
                with self._cache_lock:
                    self._pan_capability[key] = entry
        if not self._pan_capability_configured:
            return entry.get("state") != "failed"
        return entry.get("state") == "enabled"

    def _mark_provider_capability(self, provider, state, error=""):
        key = self._provider_accel_key(provider)
        if not key:
            return
        with self._cache_lock:
            current = dict(self._pan_capability.get(key) or {})
            current["state"] = str(state or "unknown")
            current["error"] = self._short_backend_error(error) if error else ""
            current["failed_until"] = (
                time.time() + self.pan_capability_failure_cooldown
                if current["state"] == "failed"
                else 0
            )
            self._pan_capability[key] = current
        self._log("pan-capability", provider=provider, state=state)

    def _provider_name(self, url, type_value=None, type_names=None):
        try:
            type_number = int(type_value)
        except Exception:
            type_number = None
        if isinstance(type_names, list) and type_number is not None:
            if 0 <= type_number < len(type_names):
                advertised = self._clean_text(type_names[type_number])
                if advertised:
                    return self._canonical_provider(advertised)
        hostname = (urlsplit(str(url or "")).hostname or "").lower()
        for domain, name in self.PROVIDER_DOMAIN_NAMES:
            if hostname == domain or hostname.endswith("." + domain):
                return name
        if type_number in self.PROVIDER_TYPE_NAMES:
            return self.PROVIDER_TYPE_NAMES[type_number]
        return "其他网盘"

    def _canonical_provider(self, value):
        text = self._clean_text(value)
        aliases = (
            ("115", "115网盘"),
            ("夸克", "夸克网盘"),
            ("阿里", "阿里网盘"),
            ("百度", "百度网盘"),
            ("迅雷", "迅雷网盘"),
            ("UC", "UC网盘"),
            ("天翼", "天翼网盘"),
            ("189", "天翼网盘"),
            ("123", "123网盘"),
            ("移动", "移动网盘"),
            ("和彩云", "移动网盘"),
            ("微云", "微云"),
        )
        upper = text.upper()
        for marker, name in aliases:
            if marker.upper() in upper:
                return name
        return text or "其他网盘"

    def _resource_remark(self, quality, magnets, pans, online):
        if isinstance(quality, list):
            quality_text = "/".join(self._clean_text(item) for item in quality if item)
        else:
            quality_text = self._clean_text(quality)
        counts = "磁%s 盘%s 线%s" % (
            self._safe_int(magnets),
            self._safe_int(pans),
            self._safe_int(online),
        )
        return (quality_text + " " + counts).strip()

    def _quality_score(self, value):
        text = str(value or "")
        if self.QUALITY_4K_RE.search(text):
            return 3
        if self.QUALITY_1080_RE.search(text):
            return 2
        if re.search(r"720|HD", text, re.I):
            return 1
        return 0

    def _time_score(self, value):
        text = self._clean_text(value)
        if text == "今天":
            return 100000
        if text == "昨天":
            return 99999
        match = re.search(r"(\d+)\s*天前", text)
        if match:
            return max(0, 99998 - int(match.group(1)))
        return 0

    def _size_bytes(self, value):
        sizes = []
        for match in re.finditer(
            r"([0-9]+(?:\.[0-9]+)?)\s*(T|G|M|K)(?:i?B)?",
            str(value or ""),
            re.I,
        ):
            number = float(match.group(1))
            unit = match.group(2).upper()
            power = {"K": 1, "M": 2, "G": 3, "T": 4}.get(unit, 0)
            sizes.append(int(number * (1024 ** power)))
        return max(sizes) if sizes else 0

    def _pack(self, payload):
        compact = {self.PACK_KEYS.get(key, key): value for key, value in payload.items()}
        raw = json.dumps(compact, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return self.PRIVATE_PREFIX + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    def _unpack(self, value):
        if not value.startswith(self.PRIVATE_PREFIX):
            return None
        raw = value[len(self.PRIVATE_PREFIX):]
        raw += "=" * (-len(raw) % 4)
        try:
            data = json.loads(base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8"))
            if not isinstance(data, dict):
                return None
            expanded = {short: full for full, short in self.PACK_KEYS.items()}
            return {expanded.get(key, key): value for key, value in data.items()}
        except Exception:
            return None

    def _status_id(self, message):
        raw = str(message or "").encode("utf-8")
        return self.STATUS_PREFIX + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    def _decode_status(self, value):
        raw = value[len(self.STATUS_PREFIX):]
        raw += "=" * (-len(raw) % 4)
        try:
            return base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8")
        except Exception:
            return "资源不可用"

    def _player_error(self, message):
        text = self._clean_text(message) or "播放资源不可用"
        return {
            "parse": 0,
            "jx": 0,
            "playUrl": "",
            "url": "",
            "header": {},
            "msg": text,
            "error": text,
            "content": text,
        }

    def _vod_id(self, dir_name, item_id):
        return "guale-v28://%s/%s" % (dir_name, item_id)

    def _vod_card(self, dir_name, item_id, title, pic, remarks=""):
        return {
            "vod_id": self._vod_id(dir_name, item_id),
            "vod_name": self._clean_text(title),
            "vod_pic": pic,
            "vod_remarks": remarks,
        }

    def _decode_vod_id(self, value):
        match = re.match(r"^guale-v28://(mv|tv|ac)/([A-Za-z0-9_-]+)$", value)
        if match:
            return match.group(1), match.group(2)
        match = re.match(r"^guale://(mv|tv|ac)/([A-Za-z0-9_-]+)$", value)
        if match:
            return match.group(1), match.group(2)
        match = re.match(r"^/?(mv|tv|ac)/([A-Za-z0-9_-]+)$", value)
        if match:
            return match.group(1), match.group(2)
        return "", ""

    def _image(self, dir_name, item_id, size=256):
        return "%s/img/%s/%s/%s.webp" % (self.IMAGE_HOST, dir_name, item_id, size)

    def _remember_page(self, key, value):
        with self._cache_lock:
            self._page_cache[key] = (time.time(), value)
            self._page_cache.move_to_end(key)
            while len(self._page_cache) > self.MAX_CACHE:
                self._page_cache.popitem(last=False)

    def _elapsed_ms(self, started):
        return max(0, int((time.perf_counter() - started) * 1000))

    def _record_metric(self, name, amount=1, elapsed_ms=None):
        snapshot = None
        now = time.time()
        with self._metrics_lock:
            self._metrics[name] = int(self._metrics.get(name) or 0) + int(amount or 0)
            if elapsed_ms is not None:
                current = dict(self._metric_latencies.get(name) or {})
                current["count"] = int(current.get("count") or 0) + 1
                current["total_ms"] = int(current.get("total_ms") or 0) + int(elapsed_ms)
                current["max_ms"] = max(int(current.get("max_ms") or 0), int(elapsed_ms))
                self._metric_latencies[name] = current
            if now - self._metrics_last_log >= self.metrics_log_interval:
                self._metrics_last_log = now
                snapshot = dict(self._metrics)
                for metric_name, latency in self._metric_latencies.items():
                    count = int(latency.get("count") or 0)
                    if count:
                        snapshot[metric_name + "_avg_ms"] = int(
                            int(latency.get("total_ms") or 0) / count
                        )
        if snapshot:
            self._log("metrics", **snapshot)

    def _metrics_snapshot(self):
        with self._metrics_lock:
            snapshot = dict(self._metrics)
            for metric_name, latency in self._metric_latencies.items():
                count = int(latency.get("count") or 0)
                if count:
                    snapshot[metric_name + "_avg_ms"] = int(
                        int(latency.get("total_ms") or 0) / count
                    )
            stats = dict(self._pan_parse_stats)
        snapshot["pan_parse_attempts"] = int(stats.get("attempts") or 0)
        snapshot["pan_parse_successes"] = int(stats.get("successes") or 0)
        snapshot["pan_parse_ewma_ms"] = int(stats.get("ewma_ms") or 0)
        return snapshot

    def _diag(self, stage, **values):
        self._last_diag = {"stage": stage, "time": int(time.time()), **values}

    def _log(self, stage, **values):
        parts = []
        for key in sorted(values):
            value = str(values[key]).replace("\r", " ").replace("\n", " ")
            if len(value) > 160:
                value = value[:157] + "..."
            parts.append("%s=%s" % (key, value))
        suffix = " " + " ".join(parts) if parts else ""
        print("[%s] %s%s" % (self.RUNTIME_TAG, stage, suffix), flush=True)

    def _is_challenge(self, source):
        text = str(source or "")
        return "powSolve" in text or "浏览器安全验证" in text or "/res/pow" in text

    def _is_login_page(self, source):
        text = str(source or "")
        return "_BT.PC.HTML('login')" in text or "您正在查看受限内容，请登录后继续" in text

    def _normalize_host(self, value):
        raw = html_lib.unescape(str(value or "")).strip()
        if raw.startswith("//"):
            raw = "https:" + raw
        parsed = urlsplit(raw)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return ""
        return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))

    def _dedupe_candidates(self, values):
        result = []
        seen = set()
        for latency, host in values:
            normalized = self._normalize_host(host)
            key = normalized.lower()
            if normalized and key not in seen:
                seen.add(key)
                result.append((self._safe_int(latency, 99999), normalized))
        result.sort(key=lambda item: item[0])
        return result

    def _array(self, data, *keys):
        if not isinstance(data, dict):
            return []
        for key in keys:
            value = data.get(key)
            if isinstance(value, list):
                return value
        return []

    def _at(self, values, index, default=None):
        try:
            return values[index]
        except Exception:
            return default

    def _join_text(self, value):
        if isinstance(value, list):
            return " / ".join(self._clean_text(item) for item in value if item)
        return self._clean_text(value)

    def _safe_label(self, value):
        text = self._clean_text(value)
        text = text.replace("$$$", " ").replace("$", " ").replace("#", " ")
        return re.sub(r"\s+", " ", text).strip()[:100]

    def _btih(self, value):
        match = self.BTIH_RE.search(str(value or ""))
        return match.group(1).lower() if match else ""

    def _page_number(self, value):
        try:
            return max(1, int(value or 1))
        except Exception:
            return 1

    def _safe_int(self, value, default=0):
        try:
            return int(value)
        except Exception:
            return default

    def _bounded_int(self, value, default, minimum, maximum):
        try:
            number = int(value)
        except Exception:
            number = int(default)
        return min(maximum, max(minimum, number))

    def _bool_value(self, value, default=False):
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() not in ("", "0", "false", "no", "off")

    def _url_value(self, value, default=""):
        normalized = self._normalize_host(value)
        return normalized or default

    def _http_url_value(self, value, default=""):
        raw = html_lib.unescape(str(value or "")).strip()
        if raw.startswith("//"):
            raw = "https:" + raw
        parsed = urlsplit(raw)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return default
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path or "/", parsed.query, ""))

    def _url_list(self, values):
        if isinstance(values, str):
            values = re.split(r"[\s,;]+", values)
        result = []
        seen = set()
        for value in values or []:
            normalized = self._normalize_host(value)
            key = normalized.lower()
            if normalized and key not in seen:
                seen.add(key)
                result.append(normalized)
        return result

    def _http_url_list(self, values):
        result = []
        seen = set()
        for value in values or []:
            normalized = self._http_url_value(value)
            key = normalized.lower()
            if normalized and key not in seen:
                seen.add(key)
                result.append(normalized)
        return result

    def _config(self, extend):
        if isinstance(extend, dict):
            return extend
        if isinstance(extend, str) and extend.strip():
            try:
                value = json.loads(extend)
                return value if isinstance(value, dict) else {}
            except Exception:
                return {}
        return {}

    def _atvp_runtime_context(self):
        frame = inspect.currentframe()
        try:
            frame = frame.f_back if frame else None
            for _ in range(8):
                if frame is None:
                    break
                owner = frame.f_locals.get("self")
                if owner is not None and owner is not self:
                    api = str(getattr(owner, "_backend_api", "") or "").strip()
                    token = str(getattr(owner, "_vod_token", "") or "").strip()
                    proxy = getattr(owner, "_localProxyConfig", None)
                    if api or token or isinstance(proxy, dict):
                        return {
                            "api": api,
                            "token": token,
                            "local_proxy_config": proxy if isinstance(proxy, dict) else {},
                        }
                frame = frame.f_back
        finally:
            del frame
        return {}

    def _clean_text(self, value):
        return re.sub(r"\s+", " ", html_lib.unescape(str(value or ""))).strip()

    def _empty_page(self, page, message=""):
        result = {"list": [], "page": page, "pagecount": page, "limit": 48, "total": 0}
        if message:
            result["msg"] = message
        return result
