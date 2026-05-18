// @name 观影
// @author cbfwq
// @description https://www.xn--wcv59z.com/
// @dependencies: axios
// @version 1.0.0
// @downloadURL https://raw.githubusercontent.com/cbfwq/OmniBox-Spider/refs/heads/main/影视/采集/观影.js

const axios = require("axios");
const https = require("https");
const OmniBox = require("omnibox_sdk");

// ========== 站点配置 ==========
const host = 'https://www.xn--wcv59z.com/';  // 目标网站
const def_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Referer': host
};

const axiosInstance = axios.create({
    httpsAgent: new https.Agent({ rejectUnauthorized: false }),
    timeout: 15000,
    headers: def_headers
});

// 弹幕API配置（可选）
const DANMU_API = process.env.DANMU_API || '';

// ========== 日志工具 ==========
const logInfo = (message, data = null) => {
    const output = data ? `${message}: ${JSON.stringify(data)}` : message;
    OmniBox.log("info", `[观影-DEBUG] ${output}`);
};

const logError = (message, error) => {
    OmniBox.log("error", `[观影-DEBUG] ${message}: ${error.message || error}`);
};

// ========== 辅助函数 ==========
const encodeMeta = (obj) => {
    try {
        return Buffer.from(JSON.stringify(obj || {}), 'utf8').toString('base64');
    } catch (_) {
        return '';
    }
};

const decodeMeta = (str) => {
    try {
        return JSON.parse(Buffer.from(str || '', 'base64').toString('utf8'));
    } catch (_) {
        return null;
    }
};

// ========== 类别列表 ==========
// 修复：补全了遗漏的 a 字母 (sync -> async)
async function getClassList() {
    try {
        const res = await axiosInstance.get(`${host}/api/v1/channels`);
        
        if (res.data && res.data.code === 200 && Array.isArray(res.data.data)) {
            return res.data.data.map(item => ({
                type_id: item.id,       // 对应导航：1 -> 电影, 2 -> 剧集, 3 -> 动漫...
                type_name: item.name    // 分类名称
            }));
        }
        
        logError('获取分类失败：接口返回格式不正确', res.data);
        return [];
    } catch (error) {
        logError('获取分类失败', error);
        return [];
    }
}

// ========== 分类视频列表 ==========
async function getVideosByClass(type_id, page) {
    try {
        const res = await axiosInstance.get(`${host}/api/v1/videos/filter`, {
            params: {
                channel_id: type_id,
                page: page,
                limit: 12,
                sort: 'latest'
            }
        });

        // 修复：去除了夹在中间的上一版冗余重复代码片段，将逻辑收拢
        if (res.data && res.data.code === 200 && res.data.data) {
            const videoList = res.data.data.list || res.data.list || [];
            if (Array.isArray(videoList)) {
                return videoList.map(item => ({
                    vod_id: item.id,
                    vod_name: item.title,
                    vod_pic: item.cover_url || item.poster || item.thumb || '',
                    vod_remarks: item.quality || item.note || '',
                    vod_score: item.rating || item.score || '',
                    vod_year: item.year || '',
                    vod_area: item.area || '',
                    vod_tags: item.genres || item.tags || [],
                    extra: {
                        views: item.views || ''
                    }
                }));
            }
        }

        logError('获取视频列表失败：接口数据结构不匹配', res.data);
        return [];
    } catch (error) {
        logError('获取视频列表失败', error);
        return [];
    }
}

// ========== 视频详情 ==========
async function getVideoDetail(vod_id) {
    try {
        const res = await axiosInstance.get(`${host}/api/v1/videos/detail`, {
            params: { id: vod_id }
        });

        if (!res.data || res.data.code !== 200 || !res.data.data) {
            logError('获取视频详情失败：接口数据格式不匹配', res.data);
            return null;
        }

        const item = res.data.data;
        const playList = {};
        
        const originalPlayUrls = item.play_urls || item.play_sources;
        
        if (originalPlayUrls && typeof originalPlayUrls === 'object' && !Array.isArray(originalPlayUrls)) {
            for (const [source, urls] of Object.entries(originalPlayUrls)) {
                if (Array.isArray(urls)) {
                    playList[source] = urls.map((url, index) => {
                        return url.includes('$') ? url : `第${String(index + 1).padStart(2, '0')}集$${url}`;
                    });
                }
            }
        } 
        else if (Array.isArray(originalPlayUrls)) {
            originalPlayUrls.forEach(source => {
                if (source.name && Array.isArray(source.episodes)) {
                    playList[source.name] = source.episodes.map(ep => {
                        return typeof ep === 'object' ? `${ep.name}$${ep.url}` : ep;
                    });
                }
            });
        }
        
        if (Object.keys(playList).length === 0 && item.play_url) {
            playList['默认线路'] = [`正片$${item.play_url}`];
        }
        
        return {
            vod_id: item.id,
            vod_name: item.title,
            vod_sub: item.sub_title || item.en_title || '',
            vod_pic: item.cover_url || item.poster || item.thumb || '',
            vod_remarks: item.quality || item.note || '',
            vod_score: item.rating || item.score || '',
            vod_year: item.year || '',
            vod_area: item.area || '',
            vod_content: item.summary || item.description || item.content || '',
            vod_actor: Array.isArray(item.actors) ? item.actors.join(', ') : (item.actor || ''),
            vod_director: Array.isArray(item.directors) ? item.directors.join(', ') : (item.director || ''),
            type_name: item.type_name || '',
            vod_play_from: Object.keys(playList).join('$$$'),
            vod_play_url: Object.values(playList).map(arr => arr.join('#')).join('$$$')
        };

    } catch (error) {
        logError('获取详情失败', error);
        return null;
    }
}

// ========== 搜索 ==========
async function search(keyword, page) {
    try {
        const res = await axiosInstance.get(`${host}/api/v1/videos/search`, {
            params: {
                q: keyword,
                page: page || 1,
                limit: 12
            }
        });
        
        if (res.data && res.data.code === 200 && res.data.data) {
            const items = Array.isArray(res.data.data) ? res.data.data : (res.data.data.list || []);
            
            return items.map(item => ({
                vod_id: item.id,
                vod_name: item.title,
                vod_pic: item.cover_url || item.poster || item.thumb || '',
                vod_remarks: item.quality || item.note || '',
                vod_score: item.rating || item.score || '',
                vod_year: item.year || '',
                vod_area: item.area || '',
                vod_tags: item.genres || item.tags || []
            }));
        }
        
        logError('搜索失败：接口数据结构不匹配', res.data);
        return [];
    } catch (error) {
        logError('搜索失败', error);
        return [];
    }
}

// ========== 弹幕相关（可选） ==========
async function getDanmu(vodName, episodeTitle) {
    if (!DANMU_API) return [];
    
    try {
        const searchName = vodName + ' ' + episodeTitle;
        const res = await axiosInstance.get(DANMU_API, {
            params: { key: searchName }
        });
        
        return res.data.list || [];
    } catch (error) {
        logError('获取弹幕失败', error);
        return [];
    }
}

// ========== 导出接口 ==========
module.exports = {
    getClassList,
    getVideosByClass,
    getVideoDetail,
    search
};
