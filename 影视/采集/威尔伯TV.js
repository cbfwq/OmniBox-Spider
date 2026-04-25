// @name 威尔伯TV
// @author 梦
// @description 刮削：已接入，弹幕：未接入，嗅探：直接返回 play.modujx11.com 直链
// @dependencies cheerio
// @version 1.0.1
// @downloadURL https://gh-proxy.org/https://github.com/Silent1566/OmniBox-Spider/raw/refs/heads/openclaw/影视/采集/威尔伯TV.js

const OmniBox = require("omnibox_sdk");
const runner = require("spider_runner");
const cheerio = require("cheerio");

const HOST = "https://wei2bo.com";
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";
const HEADERS = {
  "User-Agent": UA,
  Referer: `${HOST}/`,
  Origin: HOST,
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
};

const CATEGORY_MAP = [
  { type_id: "movie", type_name: "电影" },
  { type_id: "tv", type_name: "电视剧" },
  { type_id: "anime", type_name: "动漫" },
  { type_id: "variety", type_name: "综艺" },
];

module.exports = { home, category, detail, search, play };
runner.run(module.exports);

function log(level, message, data) {
  const suffix = typeof data === 'undefined' ? '' : ` | ${JSON.stringify(data)}`;
  return OmniBox.log(level, `[威尔伯TV] ${message}${suffix}`);
}

function absUrl(url) {
  const value = String(url || '').trim();
  if (!value) return '';
  if (/^https?:\/\//i.test(value)) return value;
  if (value.startsWith('//')) return `https:${value}`;
  if (value.startsWith('/')) return `${HOST}${value}`;
  return `${HOST}/${value}`;
}

function clean(text) {
  return String(text || '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/\s+/g, ' ')
    .trim();
}

function fetchText(url, options = {}) {
  return OmniBox.request(url, {
    method: options.method || 'GET',
    headers: {
      ...HEADERS,
      ...(options.headers || {}),
      Referer: options.referer || `${HOST}/`,
    },
    timeout: options.timeout || 20000,
    body: options.body,
  }).then((res) => {
    if (!res || Number(res.statusCode) < 200 || Number(res.statusCode) >= 400) {
      throw new Error(`HTTP ${res?.statusCode || 'unknown'} @ ${url}`);
    }
    return String(res.body || '');
  });
}

function extractCarouselList(html) {
  const list = [];
  const seen = new Set();
  const regex = /\{"id":(\d+),"vod":\{"id":(\d+),"vodName":"([^"]+)","vodPic":"([^"]+)","vodRemarks":"([^"]*)"/g;
  let match;
  while ((match = regex.exec(html)) !== null) {
    const vodId = String(match[2] || '').trim();
    if (!vodId || seen.has(vodId)) continue;
    seen.add(vodId);
    list.push({
      vod_id: vodId,
      vod_name: clean(match[3]),
      vod_pic: absUrl(match[4]),
      vod_remarks: clean(match[5]),
      vod_content: '',
    });
  }
  return list;
}

function extractGridList(html, urlPrefix) {
  const list = [];
  const seen = new Set();
  const pattern = /\{"id":(\d+),"vodName":"([^"]+)","vodPic":"([^"]+)","vodRemarks":"([^"]*)"/g;
  let match;
  while ((match = pattern.exec(html)) !== null) {
    const vodId = String(match[1] || '').trim();
    if (!vodId || seen.has(vodId)) continue;
    seen.add(vodId);
    list.push({
      vod_id: vodId,
      vod_name: clean(match[2]),
      vod_pic: absUrl(match[3]),
      vod_remarks: clean(match[4]),
      vod_url: `${HOST}${urlPrefix}/${vodId}`,
      vod_content: '',
    });
  }
  return list;
}

function parseCategoryHtml(html) {
  const urlPrefixMatch = html.match(/"urlPrefix":"\/(movie|tv|anime|variety)"/);
  const urlPrefix = urlPrefixMatch ? `/${urlPrefixMatch[1]}` : '';
  if (urlPrefix) {
    const grid = extractGridList(html, urlPrefix);
    if (grid.length) return grid;
  }
  return extractCarouselList(html);
}

function buildDetailResult(html, vodId) {
  const $ = cheerio.load(html);
  const title = clean($('title').text()).replace(/\s+在线观看.*$/, '') || clean($('meta[property="og:title"]').attr('content'));
  const poster = absUrl($('meta[property="og:image"]').attr('content'));
  const content = clean($('meta[property="og:description"]').attr('content') || $('meta[name="description"]').attr('content') || '');
  const director = clean($('a[href*="google.com/search?q=导演:"]').text() || '');
  const infoLine = clean($('.space-y-2 p.text-gray-1').first().text());
  const text = decodeRsc(html);
  const playUrlMatch = text.match(/https:\/\/play\.modujx11\.com\/[^"\s]+\.m3u8/);
  const episodeMatch = text.match(/episodes\":\[\"([^\"]+)\"\]/);
  const episodes = [];
  if (episodeMatch) {
    const [name, url] = String(episodeMatch[1] || '').split('$');
    if (name && url) episodes.push({ name, playId: url });
  }
  if (!episodes.length && playUrlMatch) {
    episodes.push({ name: '正片', playId: playUrlMatch[1] });
  }
  return {
    vod_id: String(vodId || ''),
    vod_name: title,
    vod_pic: poster,
    vod_content: content,
    vod_actor: '',
    vod_director: director || '',
    vod_remarks: infoLine,
    vod_play_sources: episodes.length ? [{ name: '正片', episodes }] : [],
  };
}

async function home() {
  try {
    const html = await fetchText(`${HOST}/`);
    const list = extractCarouselList(html);
    return { class: CATEGORY_MAP, filters: {}, list };
  } catch (e) {
    await log('error', 'home 失败', { error: e.message });
    return { class: CATEGORY_MAP, filters: {}, list: [] };
  }
}

async function category(params) {
  try {
    const typeId = String(params.categoryId || params.type_id || params.type || 'movie');
    const page = Math.max(1, parseInt(params.page || 1, 10));
    const subType = String((params.filters && params.filters.type) || params.type || '').trim();
    const path = subType ? `/video/${typeId}?type=${encodeURIComponent(subType)}` : `/video/${typeId}`;
    const html = await fetchText(`${HOST}${path}`);
    const list = parseCategoryHtml(html);
    return {
      page,
      pagecount: page + (list.length >= 12 ? 1 : 0),
      total: list.length,
      list,
    };
  } catch (e) {
    await log('error', 'category 失败', { error: e.message, params });
    return { page: 1, pagecount: 0, total: 0, list: [] };
  }
}

async function detail(params) {
  try {
    const vodId = String(params.videoId || params.vod_id || params.id || '').trim();
    const typeId = String(params.type_id || params.type || 'movie').trim();
    if (!vodId) return { list: [] };
    const html = await fetchText(`${HOST}/video/${typeId}/${vodId}`);
    const vod = buildDetailResult(html, vodId);
    return { list: [vod] };
  } catch (e) {
    await log('error', 'detail 失败', { error: e.message, params });
    return { list: [] };
  }
}

async function search(params) {
  try {
    const keyword = String(params.keyword || params.wd || params.key || '').trim();
    const page = Math.max(1, parseInt(params.page || 1, 10));
    if (!keyword) return { page, pagecount: 0, total: 0, list: [] };
    const html = await fetchText(`${HOST}/search?wd=${encodeURIComponent(keyword)}`);
    const list = [];
    const re = /\{"id":(\d+),"vodName":"([^"]+)","vodPic":"([^"]+)","vodRemarks":"([^"]*)"/g;
    let match;
    while ((match = re.exec(html)) !== null) {
      list.push({
        vod_id: String(match[1]),
        vod_name: clean(match[2]),
        vod_pic: absUrl(match[3]),
        vod_remarks: clean(match[4]),
      });
    }
    return { page, pagecount: page + (list.length >= 12 ? 1 : 0), total: list.length, list };
  } catch (e) {
    await log('error', 'search 失败', { error: e.message, params });
    return { page: 1, pagecount: 0, total: 0, list: [] };
  }
}

async function play(params) {
  try {
    const playUrl = String(params.playId || params.url || '').trim();
    if (!playUrl) return { parse: 0, urls: [] };
    return {
      parse: 0,
      flag: String(params.flag || '威尔伯TV'),
      header: { Referer: HOST, Origin: HOST, 'User-Agent': UA },
      urls: [{ name: String(params.name || '正片'), url: playUrl }],
    };
  } catch (e) {
    await log('error', 'play 失败', { error: e.message, params });
    return { parse: 0, urls: [] };
  }
}
