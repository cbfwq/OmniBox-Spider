// @name 盘尊社区
// @author 梦
// @description 网盘源：https://www.panzun.cc/ ，分类/搜索/详情/播放已接入，支持 a.7u9.cn 短链自动解析到真实网盘分享
// @dependencies cheerio
// @version 1.0.3
// @downloadURL https://gh-proxy.org/https://github.com/Silent1566/OmniBox-Spider/raw/refs/heads/main/影视/网盘/盘尊社区.js

const OmniBox = require("omnibox_sdk");
const runner = require("spider_runner");
const cheerio = require("cheerio");
const https = require("https");
const http = require("http");

// ==================== 配置区域开始 ====================
// 站点基础地址。
const BASE_URL = (process.env.PANZUN_HOST || "https://www.panzun.cc").replace(/\/$/, "");
// API 基础地址。
const API_URL = `${BASE_URL}/api`;
// 默认请求头 UA。
const UA = process.env.PANZUN_UA || "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36";
// 详情 / 分类 / 搜索缓存时间（秒）。
const PANZUN_CACHE_EX_SECONDS = Number(process.env.PANZUN_CACHE_EX_SECONDS || 1800);
// 网盘多线路白名单。
const DRIVE_TYPE_CONFIG = splitConfigList(process.env.DRIVE_TYPE_CONFIG || "quark;uc").map((s) => s.toLowerCase());
// 多线路名称配置。
const SOURCE_NAMES_CONFIG = splitConfigList(process.env.SOURCE_NAMES_CONFIG || "本地代理;服务端代理;直连");
// 是否强制允许服务端代理。
const EXTERNAL_SERVER_PROXY_ENABLED = String(process.env.EXTERNAL_SERVER_PROXY_ENABLED || "false").toLowerCase() === "true";
// 线路排序。
const DRIVE_ORDER = splitConfigList(process.env.DRIVE_ORDER || "baidu;tianyi;quark;uc;115;xunlei;ali;123pan").map((s) => s.toLowerCase());
// ==================== 配置区域结束 ====================

const CLASS_LIST = [
  { type_id: "movies", type_name: "影视" },
  { type_id: "anime", type_name: "动漫" },
  { type_id: "variety-shows", type_name: "综艺" },
  { type_id: "yy", type_name: "音乐" },
];

module.exports = { home, category, detail, search, play };
runner.run(module.exports);

function splitConfigList(value) {
  return String(value || "")
    .split(/[;,]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function absUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  if (/^https?:\/\//i.test(raw)) return raw;
  if (raw.startsWith("//")) return `https:${raw}`;
  return `${BASE_URL}${raw.startsWith("/") ? "" : "/"}${raw}`;
}

function cleanText(value) {
  return String(value || "")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&#160;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function b64Encode(obj) {
  return Buffer.from(JSON.stringify(obj || {}), "utf8").toString("base64");
}

function b64Decode(str) {
  try {
    return JSON.parse(Buffer.from(String(str || ""), "base64").toString("utf8"));
  } catch (_) {
    return {};
  }
}

function safeJsonParse(value, fallback = {}) {
  if (value == null) return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

async function getCachedJSON(key) {
  try {
    const val = await OmniBox.getCache(key);
    return safeJsonParse(val, null);
  } catch (_) {
    return null;
  }
}

async function setCachedJSON(key, value, exSeconds = PANZUN_CACHE_EX_SECONDS) {
  try {
    await OmniBox.setCache(key, JSON.stringify(value), exSeconds);
  } catch (_) {}
}

async function requestJSON(url) {
  await OmniBox.log("info", `[盘尊][json] GET ${url}`);
  const res = await OmniBox.request(url, {
    method: "GET",
    headers: {
      "User-Agent": UA,
      Accept: "application/json",
      "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
      Referer: `${BASE_URL}/`,
    },
    timeout: 20000,
  });
  const status = Number(res?.statusCode || 0);
  const body = typeof res?.body === "string" ? res.body : String(res?.body || "");
  if (status !== 200) throw new Error(`HTTP ${status} @ ${url}`);
  return safeJsonParse(body, {});
}

async function requestHeadOrGet(url) {
  await OmniBox.log("info", `[盘尊][http] ${url}`);
  for (const method of ["HEAD", "GET"]) {
    const res = await OmniBox.request(url, {
      method,
      headers: {
        "User-Agent": UA,
        Referer: `${BASE_URL}/`,
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
      },
      timeout: 20000,
      redirect: false,
    });
    const out = {
      statusCode: Number(res?.statusCode || 0),
      headers: res?.headers || {},
      body: typeof res?.body === "string" ? res.body : String(res?.body || ""),
      method,
    };
    if (out.statusCode >= 300 && out.statusCode < 400 && (out.headers?.location || out.headers?.Location)) {
      return out;
    }
    if (method === "GET") return out;
  }
  return { statusCode: 0, headers: {}, body: "", method: "GET" };
}

async function requestRedirectLocationNative(url) {
  return await new Promise((resolve, reject) => {
    try {
      const target = new URL(String(url || ""));
      const client = target.protocol === "http:" ? http : https;
      const req = client.request(target, {
        method: "HEAD",
        headers: {
          "User-Agent": UA,
          Referer: `${BASE_URL}/`,
          "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        },
      }, (res) => {
        const location = res.headers?.location || "";
        res.resume();
        resolve({
          statusCode: Number(res.statusCode || 0),
          location: String(location || "").trim(),
        });
      });
      req.setTimeout(20000, () => {
        req.destroy(new Error("native HEAD timeout"));
      });
      req.on("error", reject);
      req.end();
    } catch (error) {
      reject(error);
    }
  });
}

function inferDriveType(value = "") {
  const raw = String(value || "").toLowerCase();
  if (raw.includes("pan.quark.cn") || raw.includes("quark")) return "quark";
  if (raw.includes("drive.uc.cn") || raw === "uc") return "uc";
  if (raw.includes("pan.baidu.com") || raw.includes("baidu")) return "baidu";
  if (raw.includes("cloud.189.cn") || raw.includes("tianyi")) return "tianyi";
  if (raw.includes("aliyundrive") || raw.includes("alipan") || raw.includes("aliyun") || raw.includes("ali")) return "ali";
  if (raw.includes("115")) return "115";
  if (raw.includes("xunlei")) return "xunlei";
  if (raw.includes("123pan") || raw.includes("123684") || raw.includes("123865") || raw.includes("123912")) return "123pan";
  return raw;
}

function driveDisplayName(type = "") {
  const mapping = {
    quark: "夸克",
    uc: "UC",
    baidu: "百度",
    tianyi: "天翼",
    ali: "阿里",
    "115": "115",
    xunlei: "迅雷",
    "123pan": "123网盘",
  };
  return mapping[String(type || "").toLowerCase()] || "资源";
}

function resolveCallerSource(params = {}, context = {}) {
  return String(context?.from || params?.source || "").toLowerCase();
}

function getBaseURLHost(context = {}) {
  const baseURL = String(context?.baseURL || "").trim();
  if (!baseURL) return "";
  try {
    return new URL(baseURL).hostname.toLowerCase();
  } catch {
    return baseURL.toLowerCase();
  }
}

function isPrivateHost(hostname = "") {
  const host = String(hostname || "").toLowerCase();
  if (!host) return false;
  if (["localhost", "127.0.0.1", "::1", "0.0.0.0"].includes(host)) return true;
  if (/^(10\.|192\.168\.|169\.254\.)/.test(host)) return true;
  if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(host)) return true;
  if (host.endsWith(".local") || host.endsWith(".lan") || host.endsWith(".internal") || host.endsWith(".intra")) return true;
  if (host.includes(":")) return host.startsWith("fc") || host.startsWith("fd") || host.startsWith("fe80");
  return false;
}

function canUseServerProxy(context = {}) {
  if (EXTERNAL_SERVER_PROXY_ENABLED) return true;
  return isPrivateHost(getBaseURLHost(context));
}

function filterSourceNamesForCaller(sourceNames = [], callerSource = "", context = {}) {
  let filtered = Array.isArray(sourceNames) ? [...sourceNames] : [];
  const allowServerProxy = canUseServerProxy(context);

  if (callerSource === "web") filtered = filtered.filter((name) => name !== "本地代理");
  else if (callerSource === "emby") filtered = allowServerProxy ? filtered.filter((name) => name === "服务端代理") : filtered.filter((name) => name !== "服务端代理");
  else if (callerSource === "uz") filtered = filtered.filter((name) => name !== "本地代理");

  if (!allowServerProxy) filtered = filtered.filter((name) => name !== "服务端代理");
  return filtered.length ? filtered : ["直连"];
}

function getRouteTypes(context = {}, driveType = "") {
  const normalized = inferDriveType(driveType);
  if (DRIVE_TYPE_CONFIG.includes(normalized)) {
    const source = resolveCallerSource({}, context || {});
    return filterSourceNamesForCaller(SOURCE_NAMES_CONFIG, source, context || {});
  }
  return ["直连"];
}

function resolveRouteType(flag = "", callerSource = "", context = {}) {
  const allowServerProxy = canUseServerProxy(context);
  let routeType = "直连";
  if (callerSource === "web" || callerSource === "emby") routeType = allowServerProxy ? "服务端代理" : "直连";
  if (flag) routeType = flag.includes("-") ? flag.split("-").slice(-1)[0] : flag;
  if (!allowServerProxy && routeType === "服务端代理") routeType = "直连";
  if (callerSource === "uz" && routeType === "本地代理") routeType = "直连";
  return routeType;
}

function sortPlaySourcesByDriveOrder(playSources = []) {
  if (!Array.isArray(playSources) || playSources.length <= 1 || !DRIVE_ORDER.length) return playSources;
  const orderMap = Object.fromEntries(DRIVE_ORDER.map((name, idx) => [name, idx]));
  return [...playSources].sort((a, b) => {
    const at = inferDriveType(a?.name || "");
    const bt = inferDriveType(b?.name || "");
    return (orderMap[at] ?? 1e9) - (orderMap[bt] ?? 1e9);
  });
}

function mapDiscussionCard(item, includedMap = {}) {
  const attrs = item?.attributes || {};
  const rel = item?.relationships || {};
  const tags = Array.isArray(rel?.tags?.data) ? rel.tags.data : [];
  const tagNames = tags.map((tagRef) => includedMap[`tags:${tagRef.id}`]?.attributes?.name).filter(Boolean);
  const userRef = rel?.user?.data;
  const user = userRef ? includedMap[`users:${userRef.id}`] : null;
  return {
    vod_id: String(item?.id || ""),
    vod_name: cleanText(attrs.title || ""),
    vod_pic: String(user?.attributes?.avatarUrl || ""),
    vod_remarks: [tagNames.join("/"), attrs.createdAt ? String(attrs.createdAt).slice(0, 10) : ""].filter(Boolean).join(" | "),
  };
}

function buildIncludedMap(included = []) {
  const map = {};
  for (const item of Array.isArray(included) ? included : []) {
    if (!item?.type || !item?.id) continue;
    map[`${item.type}:${item.id}`] = item;
  }
  return map;
}

async function fetchDiscussions(tagSlug = "", offset = 0) {
  const cacheKey = `panzun:discussions:${tagSlug || 'all'}:${offset}`;
  const cached = await getCachedJSON(cacheKey);
  if (cached) return cached;
  const query = tagSlug
    ? `filter[tag]=${encodeURIComponent(tagSlug)}&page[offset]=${offset}`
    : `page[offset]=${offset}`;
  const url = `${API_URL}/discussions?${query}`;
  const data = await requestJSON(url);
  await setCachedJSON(cacheKey, data);
  return data;
}

async function searchDiscussions(keyword = "", offset = 0) {
  const cacheKey = `panzun:search:${keyword}:${offset}`;
  const cached = await getCachedJSON(cacheKey);
  if (cached) return cached;
  const url = `${API_URL}/discussions?filter[q]=${encodeURIComponent(keyword)}&page[offset]=${offset}`;
  const data = await requestJSON(url);
  await setCachedJSON(cacheKey, data);
  return data;
}

async function fetchDiscussionDetail(id) {
  const cacheKey = `panzun:detail:${id}`;
  const cached = await getCachedJSON(cacheKey);
  if (cached) return cached;
  const url = `${API_URL}/discussions/${id}`;
  const data = await requestJSON(url);
  await setCachedJSON(cacheKey, data);
  return data;
}

function extractShareLinksFromHtml(contentHtml = "") {
  const html = String(contentHtml || "");
  const links = new Set();
  const regex = /https?:\/\/(?:a\.7u9\.cn\/s\/[A-Za-z0-9]+|pan\.quark\.cn\/s\/[A-Za-z0-9]+|drive\.uc\.cn\/s\/[A-Za-z0-9]+|pan\.baidu\.com\/s\/[A-Za-z0-9_-]+(?:\?pwd=[A-Za-z0-9]+)?|cloud\.189\.cn\/t\/[A-Za-z0-9]+|www\.aliyundrive\.com\/s\/[A-Za-z0-9]+|www\.alipan\.com\/s\/[A-Za-z0-9]+|115\.com\/s\/[A-Za-z0-9]+|pan\.xunlei\.com\/s\/[A-Za-z0-9]+|www\.123684\.com\/s\/[A-Za-z0-9]+|www\.123865\.com\/s\/[A-Za-z0-9]+|www\.123912\.com\/s\/[A-Za-z0-9]+|www\.123pan\.com\/s\/[A-Za-z0-9]+)/gi;
  let match;
  while ((match = regex.exec(html))) {
    const link = String(match[0] || "").replace(/["'<>\s]+$/g, "").trim();
    if (link) links.add(link);
  }
  return [...links];
}

async function resolveShareUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  if (!/^https?:\/\/a\.7u9\.cn\/s\//i.test(raw)) return raw;
  try {
    const nativeHead = await requestRedirectLocationNative(raw);
    if (nativeHead?.location) {
      await OmniBox.log("info", `[盘尊][resolve-share] ${raw} -> ${nativeHead.location} (native-head)`);
      return nativeHead.location;
    }
  } catch (e) {
    await OmniBox.log("warn", `[盘尊][resolve-share] ${raw} -> native-head失败: ${e.message || e}`);
  }
  try {
    const res = await requestHeadOrGet(raw);
    const location = res?.headers?.location || res?.headers?.Location || "";
    if (location) {
      const finalUrl = String(location).trim();
      await OmniBox.log("info", `[盘尊][resolve-share] ${raw} -> ${finalUrl} (${res.method || 'request'})`);
      return finalUrl;
    }
    const body = String(res?.body || "");
    const m = body.match(/https?:\/\/(?:pan\.quark\.cn|drive\.uc\.cn|pan\.baidu\.com|cloud\.189\.cn|www\.aliyundrive\.com|www\.alipan\.com|115\.com|pan\.xunlei\.com|www\.123684\.com|www\.123865\.com|www\.123912\.com|www\.123pan\.com)[^"'<>\s]+/i);
    if (m?.[0]) {
      await OmniBox.log("info", `[盘尊][resolve-share] ${raw} -> ${m[0]} (body)`);
      return m[0];
    }
  } catch (e) {
    await OmniBox.log("warn", `[盘尊][resolve-share] ${raw} -> 解析失败: ${e.message || e}`);
  }
  await OmniBox.log("warn", `[盘尊][resolve-share] ${raw} -> 保留原地址`);
  return raw;
}

async function getDriveInfo(shareUrl) {
  return await OmniBox.getDriveInfoByShareURL(shareUrl);
}

async function getDriveFileList(shareUrl, fid = "0") {
  return await OmniBox.getDriveFileList(shareUrl, fid);
}

function normalizeItems(data) {
  if (Array.isArray(data)) return data;
  if (data && typeof data === "object") {
    for (const key of ["items", "list", "files", "data", "result"]) {
      const value = data[key];
      if (Array.isArray(value)) return value;
      if (value && typeof value === "object") {
        for (const subKey of ["items", "list", "files"]) {
          if (Array.isArray(value[subKey])) return value[subKey];
        }
      }
    }
  }
  return [];
}

function isFolder(item = {}) {
  return Boolean(item?.isFolder || item?.dir || item?.folder || item?.type === "folder" || item?.category === "folder");
}

function fileNameOf(item = {}) {
  return String(item?.name || item?.file_name || item?.fileName || item?.title || "").trim();
}

function fileIdOf(item = {}) {
  return String(item?.fileId || item?.fid || item?.id || item?.shareId || "").trim();
}

function isVideoFile(item = {}) {
  const name = fileNameOf(item).toLowerCase();
  return /\.(mp4|mkv|avi|mov|wmv|flv|m4v|ts|m2ts|webm|mpg|mpeg)$/i.test(name) || String(item?.mimeType || "").startsWith("video/");
}

async function collectDriveVideos(shareUrl, fid = "0", depth = 0, visited = new Set()) {
  if (depth > 3) return [];
  const key = `${shareUrl}@@${fid}`;
  if (visited.has(key)) return [];
  visited.add(key);

  const raw = await getDriveFileList(shareUrl, fid);
  const items = normalizeItems(raw);
  const videos = [];

  for (const item of items) {
    if (isFolder(item)) {
      const childId = fileIdOf(item);
      if (childId) videos.push(...await collectDriveVideos(shareUrl, childId, depth + 1, visited));
      continue;
    }
    if (isVideoFile(item)) {
      videos.push({
        fid: fileIdOf(item),
        name: fileNameOf(item),
        size: Number(item?.size || item?.file_size || item?.obj_size || 0),
      });
    }
  }
  return videos;
}

function buildDrivePlayMeta(shareUrl, file, routeType) {
  return {
    kind: "drive",
    shareUrl,
    fid: String(file?.fid || ""),
    name: String(file?.name || ""),
    routeType: String(routeType || "直连"),
    size: Number(file?.size || 0),
  };
}

async function home() {
  try {
    const data = await fetchDiscussions("", 0);
    const includedMap = buildIncludedMap(data?.included || []);
    const list = (Array.isArray(data?.data) ? data.data : [])
      .map((item) => mapDiscussionCard(item, includedMap))
      .filter((item) => item.vod_id && item.vod_name)
      .slice(0, 20);
    await OmniBox.log("info", `[盘尊][home] count=${list.length}`);
    return { class: CLASS_LIST, filters: {}, list };
  } catch (e) {
    await OmniBox.log("error", `[盘尊][home] ${e.message || e}`);
    return { class: CLASS_LIST, filters: {}, list: [] };
  }
}

async function category(params = {}) {
  try {
    const categoryId = String(params.categoryId || params.type_id || "movies");
    const page = Math.max(1, Number(params.page || 1));
    const offset = (page - 1) * 20;
    const data = await fetchDiscussions(categoryId, offset);
    const includedMap = buildIncludedMap(data?.included || []);
    const list = (Array.isArray(data?.data) ? data.data : []).map((item) => mapDiscussionCard(item, includedMap)).filter((item) => item.vod_id && item.vod_name);
    const hasMore = Boolean(data?.links?.next);
    return {
      page,
      pagecount: hasMore ? page + 1 : page,
      total: offset + list.length + (hasMore ? 1 : 0),
      list,
    };
  } catch (e) {
    await OmniBox.log("error", `[盘尊][category] ${e.message || e}`);
    return { page: 1, pagecount: 1, total: 0, list: [] };
  }
}

async function search(params = {}) {
  try {
    const keyword = String(params.keyword || params.wd || "").trim();
    const page = Math.max(1, Number(params.page || 1));
    if (!keyword) return { page: 1, pagecount: 1, total: 0, list: [] };
    const offset = (page - 1) * 20;
    const data = await searchDiscussions(keyword, offset);
    const includedMap = buildIncludedMap(data?.included || []);
    const list = (Array.isArray(data?.data) ? data.data : []).map((item) => mapDiscussionCard(item, includedMap)).filter((item) => item.vod_id && item.vod_name);
    const hasMore = Boolean(data?.links?.next);
    return {
      page,
      pagecount: hasMore ? page + 1 : page,
      total: offset + list.length + (hasMore ? 1 : 0),
      list,
    };
  } catch (e) {
    await OmniBox.log("error", `[盘尊][search] ${e.message || e}`);
    return { page: 1, pagecount: 1, total: 0, list: [] };
  }
}

async function detail(params = {}, context = {}) {
  try {
    const discussionId = String(params.videoId || params.vod_id || params.id || "").trim();
    if (!discussionId) return { list: [] };
    const data = await fetchDiscussionDetail(discussionId);
    const includedMap = buildIncludedMap(data?.included || []);
    const discussion = data?.data || {};
    const attrs = discussion?.attributes || {};
    const rel = discussion?.relationships || {};
    const tags = Array.isArray(rel?.tags?.data) ? rel.tags.data : [];
    const tagNames = tags.map((tagRef) => includedMap[`tags:${tagRef.id}`]?.attributes?.name).filter(Boolean);
    const userRef = rel?.user?.data;
    const user = userRef ? includedMap[`users:${userRef.id}`] : null;

    const firstPostRef = Array.isArray(rel?.posts?.data) ? rel.posts.data[0] : null;
    const firstPost = firstPostRef ? includedMap[`posts:${firstPostRef.id}`] : null;
    const contentHtml = String(firstPost?.attributes?.contentHtml || "");
    const $ = cheerio.load(contentHtml || "");
    const paragraphs = $("p").map((_, el) => cleanText($(el).html() || $(el).text())).get().filter(Boolean);
    const content = paragraphs.join("\n\n");

    let shareLinks = extractShareLinksFromHtml(contentHtml);
    const resolvedLinks = [];
    for (const link of shareLinks) {
      const resolved = await resolveShareUrl(link);
      if (resolved) resolvedLinks.push(resolved);
    }
    shareLinks = [...new Set(resolvedLinks)];

    const sources = [];
    const totalShares = shareLinks.length;
    for (let index = 0; index < shareLinks.length; index++) {
      const shareUrl = shareLinks[index];
      const driveType = inferDriveType(shareUrl);
      const baseName = `${driveDisplayName(driveType)}${totalShares > 1 ? index + 1 : ""}`;
      const videos = await collectDriveVideos(shareUrl, "0");
      const routeTypes = getRouteTypes(context, driveType);
      if (videos.length) {
        for (const routeType of routeTypes) {
          const episodes = videos.map((file, i) => ({
            name: file.name || `文件${i + 1}`,
            playId: b64Encode(buildDrivePlayMeta(shareUrl, file, routeType)),
            size: file.size || 0,
          }));
          sources.push({
            name: routeTypes.length > 1 ? `${baseName}-${routeType}` : baseName,
            episodes,
          });
        }
      } else {
        sources.push({
          name: baseName,
          episodes: [{ name: baseName, playId: b64Encode({ kind: "link", url: shareUrl, name: baseName }) }],
        });
      }
    }

    const item = {
      vod_id: discussionId,
      vod_name: cleanText(attrs.title || ""),
      vod_pic: String(user?.attributes?.avatarUrl || ""),
      vod_content: content,
      vod_remarks: [tagNames.join("/"), attrs.createdAt ? String(attrs.createdAt).slice(0, 10) : ""].filter(Boolean).join(" | "),
      type_name: tagNames.join("/"),
      vod_play_sources: sortPlaySourcesByDriveOrder(sources),
    };
    return { list: [item] };
  } catch (e) {
    await OmniBox.log("error", `[盘尊][detail] ${e.message || e}`);
    return { list: [] };
  }
}

function normalizePlayResult(playInfo, meta = {}) {
  const input = playInfo && typeof playInfo === "object" ? { ...playInfo } : {};
  let urls = Array.isArray(input.urls) ? input.urls : [];
  let url = input.url;

  if (Array.isArray(url)) {
    urls = url;
    url = "";
  }

  if (urls.length && typeof urls[0] === "object") {
    const mapped = urls
      .map((item, idx) => {
        if (!item) return null;
        if (typeof item === "string") return { name: `播放${idx + 1}`, url: item };
        const candidate = item.url || item.src || item.playUrl || item.link || item.file || "";
        if (!candidate) return null;
        return { name: item.name || item.label || item.title || `播放${idx + 1}`, url: candidate };
      })
      .filter(Boolean);
    urls = mapped;
  }

  if (!urls.length && typeof url === "string" && url) {
    urls = [{ name: meta.name || "播放", url }];
  }

  if (!url && urls.length) {
    url = String(urls[0].url || "");
  }

  return {
    ...input,
    parse: Number.isFinite(Number(input.parse)) ? Number(input.parse) : 0,
    url: typeof url === "string" ? url : "",
    urls,
    header: input.header || {},
  };
}

async function play(params = {}, context = {}) {
  try {
    const rawPlayId = String(params.playId || params.id || "");
    const meta = b64Decode(rawPlayId);
    if (meta?.kind === "link") {
      await OmniBox.log("info", `[盘尊][play] direct-link ${meta.url || ''}`);
      return { parse: 0, url: meta.url || "", urls: [{ name: meta.name || "资源", url: meta.url || "" }], header: {} };
    }
    if (meta?.kind !== "drive") return { parse: 0, url: "", urls: [], header: {} };
    const callerSource = resolveCallerSource(params, context);
    const routeType = resolveRouteType(params.flag || meta.routeType || "", callerSource, context);
    await OmniBox.log("info", `[盘尊][play] share=${meta.shareUrl || ''} fid=${meta.fid || ''} route=${routeType}`);
    const playInfo = await OmniBox.getDriveVideoPlayInfo(meta.shareUrl, meta.fid, routeType);
    const normalized = normalizePlayResult(playInfo, meta);
    await OmniBox.log("info", `[盘尊][play] out parse=${normalized.parse} url=${normalized.url || ''} urls=${Array.isArray(normalized.urls) ? normalized.urls.length : 0}`);
    return normalized;
  } catch (e) {
    await OmniBox.log("error", `[盘尊][play] ${e.message || e}`);
    return { parse: 0, url: "", urls: [], header: {} };
  }
}
