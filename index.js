// index.js (ESM)
// package.json に "type": "module" がある前提

import express from "express";
import dotenv from 'dotenv';
dotenv.config();

const app = express();
const PORT = Number(process.env.PORT || 5000);

app.get('/', (_req, res) => {
    res.status(200).send('ok');
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`Healthcheck server listening on ${PORT}`);
});

process.on('unhandledRejection', (reason) => {
    console.error('unhandledRejection:', reason);
});

import {
    Client,
    GatewayIntentBits,
    Partials,
    REST,
    Routes,
    SlashCommandBuilder,
    PermissionFlagsBits,
    ActionRowBuilder,
    ButtonBuilder,
    ButtonStyle,
    UserSelectMenuBuilder,
    StringSelectMenuBuilder,
    ModalBuilder,
    TextInputBuilder,
    TextInputStyle,
    EmbedBuilder,
    Events,
} from 'discord.js';

console.log('ENV CHECK', {
    tokenLen: (process.env.DISCORD_TOKEN || '').length,
    dbName: process.env.DB_CHANNEL_NAME,
});

const TOKEN = process.env.DISCORD_TOKEN;
const DB_CHANNEL_NAME = (process.env.DB_CHANNEL_NAME || 'gourmet-db').toLowerCase();

console.log('TOKEN loaded?', !!TOKEN, 'DB_CHANNEL_NAME=', DB_CHANNEL_NAME);
if (!TOKEN) throw new Error('DISCORD_TOKEN is required');

const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent, // 写真添付拾うため
    ],
    partials: [Partials.Channel, Partials.Message],
});

// ====== 保存方式（DBなし：保存用チャンネルのEmbedが正本） ======
const DATA_MARK = '__DATA__:';

// guildId -> Map(postId -> post)
const cacheByGuild = new Map();
// guildId -> boolean
const cacheReadyByGuild = new Map();

// 新規/編集ドラフト
// key: guildId:userId -> {
//   mode:'create'|'edit',
//   postId?:string|null,
//   visited:boolean|null,
//   rating:number|null,
//   comment:string,
//   prefecture:string,
//   visitedDate:string,
//   tags:string[],
//   url:string,
//   mapUrl:string,
//   name:string,
//   channelId:string,
// }
const draftRating = new Map();

// ユーザーごとのephemeral UIメッセージ管理
// key: guildId:userId -> Set(messageId)
const uiMessages = new Map();

// 詳細画面の写真表示状態 key: guildId:userId -> { postId, idx }
const detailPhotoView = new Map();

// 検索状態 key: guildId:userId
// { userIdFilter?:string[]|null, prefectureFilters?:string[], tagFilters?:string[], keyword?:string, results?:string[], page?:number, ratingFilters?:number[] }
const searchState = new Map();

// 自分の記録（カード一覧）状態 key: guildId:userId
// { results?:string[], page?:number, visitFilter?:'all'|'visited'|'planned' }
const mineState = new Map();

// 写真追加待ち key: guildId:userId
// { postId, channelId, guildId, uiMessageRef?, backTo?: 'home'|'detail' }
const awaitingPhoto = new Map();

// 写真ビュー状態 key: guildId:userId
// { postId, idx }
const photoView = new Map();

// 詳細画面の戻り先状態 key: guildId:userId -> { postId, fromMine, forceHomeBack }
const detailNavState = new Map();

// 検索キーワードモーダルを閉じたあと元の検索パネルを更新するため
// key: guildId:userId -> { webhook, messageId }
const searchKeywordPromptRef = new Map();

// 新規登録パネル参照
// key: guildId:userId -> { webhook, messageId }
const createPanelPromptRef = new Map();

// 編集パネル参照
// key: guildId:userId -> { webhook, messageId }
const editPanelPromptRef = new Map();

// ====== util ======
function nowIso() {
    return new Date().toISOString();
}

function formatJst(date) {
    if (!date) return '';
    return new Date(date).toLocaleString('ja-JP', {
        timeZone: 'Asia/Tokyo',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
    });
}

function setDetailNavState(guildId, userId, postId, { fromMine = false, forceHomeBack = false } = {}) {
    const k = keyOf(guildId, userId);
    detailNavState.set(k, {
        postId,
        fromMine,
        forceHomeBack,
    });
}

function getDetailNavState(guildId, userId, postId) {
    const k = keyOf(guildId, userId);
    const st = detailNavState.get(k);
    if (st && st.postId === postId) return st;
    return null;
}

async function blankCurrentMessage(interaction) {
    try {
        if (!interaction?.message?.id) return;
        await interaction.webhook.deleteMessage(interaction.message.id);
    } catch (e) {
        console.error('blankCurrentMessage failed:', e);
    }
}

async function blankMessageById(interaction, messageId) {
    try {
        if (!messageId) return;
        await interaction.webhook.deleteMessage(messageId);
    } catch (e) {
        console.error('blankMessageById failed:', e);
    }
}

async function blankPromptRef(ref) {
    try {
        if (!ref?.webhook || !ref?.messageId) return;
        await ref.webhook.deleteMessage(ref.messageId);
    } catch (e) {
        console.error('blankPromptRef failed:', e);
    }
}

async function editPromptRef(ref, payload) {
    try {
        if (!ref?.webhook || !ref?.messageId) return false;
        await ref.webhook.editMessage(ref.messageId, payload);
        return true;
    } catch (e) {
        console.error('editPromptRef failed:', e);
        return false;
    }
}

async function deletePromptRef(ref) {
    try {
        if (!ref?.webhook || !ref?.messageId) return true;
        await ref.webhook.deleteMessage(ref.messageId);
        return true;
    } catch (e) {
        console.error('deletePromptRef failed:', e);
        return false;
    }
}

async function clearEphemeralMessage(interaction) {
    try {
        if (interaction?.message?.id) {
            await interaction.webhook.deleteMessage(interaction.message.id);
        }
    } catch { }
}

function tagEntryChoiceComponents(mode, guildId, userId) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`${mode}:tagsExisting:${guildId}:${userId}`)
                .setLabel('既存タグから選ぶ')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`${mode}:tagsNew:${guildId}:${userId}`)
                .setLabel('新規追加')
                .setStyle(ButtonStyle.Primary),

            new ButtonBuilder()
                .setCustomId(`${mode}:panelBack:${guildId}:${userId}`)
                .setLabel('戻る')
                .setStyle(ButtonStyle.Secondary),
        ),
    ];
}

function tagPickComponents(mode, guildId, userId, cache, selectedTags = [], page = 0) {
    const { p, totalPages, slice } = tagSlice(cache, page);

    const options = (slice.length ? slice : ['(タグなし)']).map(x => ({
        label: x,
        value: x,
        default: selectedTags.includes(x),
    }));

    const select = new StringSelectMenuBuilder()
        .setCustomId(`${mode}:tagPick:${guildId}:${userId}:${p}`)
        .setPlaceholder(`タグを選択してください（複数可） ${p + 1}/${totalPages}`)
        .setMinValues(0)
        .setMaxValues(Math.max(1, options.length))
        .addOptions(options);

    return [
        new ActionRowBuilder().addComponents(select),
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`${mode}:tagPagePrev:${guildId}:${userId}:${p}`)
                .setLabel('◀ 前へ')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(p <= 0),

            new ButtonBuilder()
                .setCustomId(`${mode}:tagPageNext:${guildId}:${userId}:${p}`)
                .setLabel('次へ ▶')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(p >= totalPages - 1),

            new ButtonBuilder()
                .setCustomId(`${mode}:tagClear:${guildId}:${userId}`)
                .setLabel('解除')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`${mode}:tagChoiceBack:${guildId}:${userId}`)
                .setLabel('戻る')
                .setStyle(ButtonStyle.Secondary),
        ),
    ];
}

function addUiMessageId(guildId, userId, messageId) {
    const k = keyOf(guildId, userId);
    if (!uiMessages.has(k)) uiMessages.set(k, new Set());
    uiMessages.get(k).add(messageId);
}

async function rememberUiReply(interaction, guildId, userId) {
    try {
        const msg = await interaction.fetchReply();
        if (msg?.id) addUiMessageId(guildId, userId, msg.id);
        return msg;
    } catch {
        return null;
    }
}

async function clearOtherUiMessages(interaction, guildId, userId, keepMessageId = null) {
    const k = keyOf(guildId, userId);
    const ids = [...(uiMessages.get(k) ?? new Set())];

    for (const mid of ids) {
        if (keepMessageId && mid === keepMessageId) continue;
        try {
            await interaction.webhook.deleteMessage(mid);
        } catch { }
    }

    if (keepMessageId) {
        uiMessages.set(k, new Set([keepMessageId]));
    } else {
        uiMessages.delete(k);
    }
}

function imageUrls(post) {
    const imgs = post.images ?? [];
    return imgs.map(x => (typeof x === 'string' ? x : x?.url)).filter(Boolean);
}

function buildDetailEmbedsChunks(post, { sharedByUserId = null } = {}) {
    const top = [];

    // 行った / 行きたい
    top.push(visitLabel(post));

    // 行った時だけ評価
    if (post.visited !== false) {
        top.push(hasRating(post) ? stars(post.rating) : '評価なし');
    }

    // コメント
    if (post.comment) {
        top.push('');
        top.push(post.comment);
    }

    const body = [
        ...top,
        '',
        `🗾 ${post.prefecture ? post.prefecture : '(未設定)'}`,
        ...(post.visited !== false && post.visited_date ? [`📅 ${post.visited_date}`] : []),
        `🏷 ${tagString(post.tags)}`,
        `👤 登録者 <@${post.created_by}>`,
        ...(sharedByUserId ? [`📤 共有 <@${sharedByUserId}>`] : []),
    ].join('\n');

    const info = new EmbedBuilder()
        .setTitle(`🍽 ${post.name}`)
        .setDescription(body)
        .addFields(
            { name: '🔗 Webサイト', value: post.url || '(なし)' },
            { name: '📍 場所', value: post.map_url || '(なし)' }
        );

    if (post.updated_at) {
        info.setFooter({ text: `更新: ${formatJst(post.updated_at)}` });
    }

    const images = imageUrls(post);

    const imageEmbeds = images.map((url, i) =>
        new EmbedBuilder()
            .setTitle(`📷 写真 ${i + 1}/${images.length}`)
            .setImage(url)
    );

    const chunks = [];
    chunks.push([info, ...imageEmbeds.slice(0, 9)]);

    let i = 9;
    while (i < imageEmbeds.length) {
        chunks.push(imageEmbeds.slice(i, i + 10));
        i += 10;
    }

    return chunks;
}

function keyOf(guildId, userId) {
    return `${guildId}:${userId}`;
}

function getGuildCache(guildId) {
    if (!cacheByGuild.has(guildId)) cacheByGuild.set(guildId, new Map());
    return cacheByGuild.get(guildId);
}

function stars(rating) {
    const r = Math.max(1, Math.min(5, Number(rating) || 1));
    return '⭐'.repeat(r) + '☆'.repeat(5 - r);
}

function visitLabel(post) {
    return post?.visited === false ? '📝 行きたい' : '✅ 行った';
}

function hasRating(post) {
    return post?.visited !== false && post?.rating != null;
}

function visitFilterMatch(state, post) {
    const filter = state?.visitFilter ?? 'all';

    if (filter === 'visited') return post.visited !== false;
    if (filter === 'planned') return post.visited === false;
    return true; // all
}

function normalizeVisitedDate(raw) {
    const s = (raw ?? '').trim();
    if (!s) return '';

    // YYYY-MM-DD → YYYY/MM/DD に寄せる
    const m = s.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})$/);
    if (!m) return null;

    const y = Number(m[1]);
    const mo = Number(m[2]);
    const d = Number(m[3]);

    const dt = new Date(y, mo - 1, d);
    if (
        dt.getFullYear() !== y ||
        dt.getMonth() !== mo - 1 ||
        dt.getDate() !== d
    ) {
        return null;
    }

    const mm = String(mo).padStart(2, '0');
    const dd = String(d).padStart(2, '0');
    return `${y}/${mm}/${dd}`;
}

function todayYMD() {
    const d = new Date();
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}/${m}/${day}`;
}

function parseTags(raw) {
    const arr = (raw ?? '')
        .split(',')
        .map(s => s.trim())
        .filter(Boolean)
        .map(s => (s.startsWith('#') ? s.slice(1).trim() : s))
        .filter(Boolean);

    const seen = new Set();
    const out = [];
    for (const t of arr) {
        const k = t.toLowerCase();
        if (!seen.has(k)) {
            seen.add(k);
            out.push(t);
        }
    }
    return out;
}

function tagString(tags) {
    if (!tags?.length) return '(なし)';
    return tags.map(t => `#${t}`).join(' ');
}

function isImageAttachment(att) {
    const ct = att.contentType || '';
    if (ct.startsWith('image/')) return true;
    const name = (att.name || '').toLowerCase();
    return (
        name.endsWith('.png') ||
        name.endsWith('.jpg') ||
        name.endsWith('.jpeg') ||
        name.endsWith('.webp') ||
        name.endsWith('.gif')
    );
}

async function getDbChannelForGuild(guild) {
    const channels = await guild.channels.fetch();

    let ch = channels.find(
        c =>
            c &&
            c.type === 0 &&
            c.name?.toLowerCase() === DB_CHANNEL_NAME
    );

    if (ch) return ch;

    const everyone = guild.roles.everyone;

    ch = await guild.channels.create({
        name: DB_CHANNEL_NAME,
        type: 0,
        reason: 'グルメ記録DBチャンネル自動作成',

        permissionOverwrites: [
            {
                id: everyone.id,
                deny: ['ViewChannel'],   // 全員見えない
            },
            {
                id: guild.members.me.id, // Bot
                allow: [
                    'ViewChannel',
                    'SendMessages',
                    'ReadMessageHistory',
                    'EmbedLinks',
                    'AttachFiles',
                    'ManageMessages'
                ],
            },
        ],
    });

    return ch;
}

function buildPostEmbedForView(post, { sharedByUserId = null, imageIndex = null, notice = null } = {}) {
    const lines = [];

    if (notice) {
        lines.push(`📤 ${notice}`);
        lines.push('');
    }

    lines.push(visitLabel(post));

    if (hasRating(post)) {
        lines.push(stars(post.rating));
        lines.push('');
    }

    if (post.comment) {
        lines.push(post.comment);
        lines.push('');
    }

    lines.push(`🗾 ${post.prefecture ? post.prefecture : '(未設定)'}`);

    if (post.visited !== false && post.visited_date) {
        lines.push(`📅 ${post.visited_date}`);
    }

    lines.push(`🏷 ${tagString(post.tags)}`);
    lines.push(`👤 登録者 <@${post.created_by}>`);

    if (sharedByUserId) {
        lines.push(`📤 共有 <@${sharedByUserId}>`);
    }

    const urls = imageUrls(post);
    if (urls.length) {
        const idx = Math.max(0, Math.min(urls.length - 1, Number(imageIndex) || 0));
        lines.push(`📷 写真 ${idx + 1}/${urls.length}`);
    }

    const fields = [
        { name: '🔗 Webサイト', value: post.url || '(なし)' },
        { name: '📍 場所', value: post.map_url || '(なし)' },
    ];

    const e = new EmbedBuilder()
        .setTitle(`🍽 ${post.name}`)
        .setDescription(lines.join('\n'))
        .addFields(fields);

    if (post.updated_at) {
        e.setFooter({ text: `更新: ${formatJst(post.updated_at)}` });
    }

    if (urls.length) {
        const idx = Math.max(0, Math.min(urls.length - 1, Number(imageIndex) || 0));
        e.setImage(urls[idx]);
    }

    return e;
}

function buildPostEmbedForDb(post) {
    const e = buildPostEmbedForView(post);

    // 保存用JSONから images を除外
    const { images, ...postNoImages } = post;

    const baseDesc = e.data.description ?? '';
    e.setDescription(baseDesc + `\n${DATA_MARK}${JSON.stringify(postNoImages)}`);
    return e;
}

function buildPostEmbed(post, { sharedByUserId = null } = {}) {
    const e = new EmbedBuilder()
        .setTitle(`🍽 ${post.name}`)
        .setDescription(
            `${stars(post.rating)}\n\n` +
            `${post.comment}\n\n` +
            `🏷 ${tagString(post.tags)}\n` +
            `👤 登録者 <@${post.created_by}>\n` +
            (sharedByUserId ? `📤 共有 <@${sharedByUserId}>\n` : '') +
            `\n${DATA_MARK}${JSON.stringify(post)}`
        )
        .addFields({ name: '🔗 Webサイト', value: post.url || '(なし)' })
        .setFooter({ text: `ID: ${post.id}  更新: ${formatJst(post.updated_at)}` });

    // 詳細は最後の画像をメインに
    const urls = imageUrls(post);
    const mainImage = urls.length ? urls[urls.length - 1] : null;
    if (mainImage) e.setImage(mainImage);
    return e;
}

// 自分の記録一覧（カード）
function buildCardEmbed(post) {
    const lines = [visitLabel(post)];

    if (hasRating(post)) {
        lines.push(stars(post.rating));
    }

    lines.push(`🗾 ${post.prefecture ? post.prefecture : '(未設定)'}`);

    if (post.visited !== false && post.visited_date) {
        lines.push(`📅 ${post.visited_date}`);
    }

    lines.push(`🏷 ${tagString(post.tags)}`);

    const e = new EmbedBuilder()
        .setTitle(`🍽 ${post.name}`)
        .setDescription(lines.join('\n'));

    const urls = imageUrls(post);
    const thumb = urls.length ? urls[urls.length - 1] : null;
    if (thumb) e.setThumbnail(thumb);

    return e;
}

function buildSearchCardEmbed(post) {
    const lines = [visitLabel(post)];

    if (hasRating(post)) {
        lines.push(stars(post.rating));
    }

    lines.push(`🗾 ${post.prefecture ? post.prefecture : '(未設定)'}`);

    if (post.visited !== false && post.visited_date) {
        lines.push(`📅 ${post.visited_date}`);
    }

    lines.push(`🏷 ${tagString(post.tags)}`);
    lines.push(`👤 登録者 <@${post.created_by}>`);

    const e = new EmbedBuilder()
        .setTitle(`🍽 ${post.name}`)
        .setDescription(lines.join('\n'));

    const urls = imageUrls(post);
    const thumb = urls.length ? urls[urls.length - 1] : null;
    if (thumb) e.setThumbnail(thumb);

    return e;
}

function extractPostFromMessage(msg) {
    if (!msg?.embeds?.length) return null;
    const emb = msg.embeds[0];
    const desc = emb.description || '';
    const idx = desc.lastIndexOf(DATA_MARK);
    if (idx < 0) return null;
    const json = desc.slice(idx + DATA_MARK.length).trim();
    try {
        const post = JSON.parse(json);
        if (!post?.id || post.id !== msg.id) post.id = msg.id;
        return post;
    } catch {
        return null;
    }
}

async function ensureCacheLoadedForGuild(guild) {
    const guildId = guild.id;
    if (cacheReadyByGuild.get(guildId)) return;

    const dbCh = await getDbChannelForGuild(guild);
    if (!dbCh?.isTextBased()) throw new Error(`#${DB_CHANNEL_NAME} はテキストチャンネルである必要があります`);

    const cache = getGuildCache(guildId);

    const imgMap = new Map(); // postId -> [{url,msgId,ts}]
    let lastId = undefined;
    let scanned = 0;

    while (true) {
        const fetched = await dbCh.messages.fetch({ limit: 100, before: lastId });
        if (!fetched.size) break;

        for (const [, msg] of fetched) {
            if (msg.author?.id !== client.user.id) continue;
            scanned++;

            // __IMG__ はここで拾う
            if (msg.content?.startsWith('__IMG__:')) {
                const parts = msg.content.split(':'); // ["__IMG__", postId, iso]
                const postId = parts[1];
                const att = msg.attachments.first();
                if (postId && att?.url) {
                    if (!imgMap.has(postId)) imgMap.set(postId, []);
                    imgMap.get(postId).push({
                        url: att.url,
                        msgId: msg.id,
                        ts: msg.createdTimestamp,
                    });
                }
                continue;
            }

            // 正本embed
            const post = extractPostFromMessage(msg);
            if (!post) continue;
            cache.set(post.id, post);
        }

        lastId = fetched.last().id;
    }

    // 画像をpostへ復元（時系列順）
    for (const [postId, list] of imgMap) {
        const post = cache.get(postId);
        if (!post) continue;
        list.sort((a, b) => a.ts - b.ts);
        post.images = list; // オブジェクト配列で保持
        cache.set(postId, post);
    }

    cacheReadyByGuild.set(guildId, true);
    console.log(`Cache loaded for guild ${guildId}: ${cache.size} posts (scanned ${scanned} bot posts)`);
}

function canEdit(interaction, post) {
    if (!interaction.guild) return false;
    if (interaction.user.id === post.created_by) return true;
    return interaction.memberPermissions?.has(PermissionFlagsBits.Administrator) ?? false;
}

function isMine(interaction, post) {
    return interaction.user?.id && post?.created_by === interaction.user.id;
}

const PREFS = [
    '北海道', '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
    '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県',
    '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県',
    '岐阜県', '静岡県', '愛知県', '三重県',
    '滋賀県', '京都府', '大阪府', '兵庫県', '奈良県', '和歌山県',
    '鳥取県', '島根県', '岡山県', '広島県', '山口県',
    '徳島県', '香川県', '愛媛県', '高知県',
    '福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県'
];

const PREF_PAGE_SIZE = 25;

function prefSlice(page = 0) {
    const totalPages = Math.ceil(PREFS.length / PREF_PAGE_SIZE);
    const p = Math.max(0, Math.min(totalPages - 1, Number(page) || 0));
    const start = p * PREF_PAGE_SIZE;
    return { p, totalPages, slice: PREFS.slice(start, start + PREF_PAGE_SIZE) };
}

const TAG_PAGE_SIZE = 25;

function getAllTagsFromCache(cache) {
    const seen = new Set();
    const out = [];

    for (const post of cache.values()) {
        for (const tag of (post.tags ?? [])) {
            const t = String(tag ?? '').trim();
            if (!t) continue;

            const key = t.toLowerCase();
            if (seen.has(key)) continue;

            seen.add(key);
            out.push(t);
        }
    }

    out.sort((a, b) => a.localeCompare(b, 'ja'));
    return out;
}

function tagSlice(cache, page = 0) {
    const tags = getAllTagsFromCache(cache);
    const totalPages = Math.max(1, Math.ceil(tags.length / TAG_PAGE_SIZE));
    const p = Math.max(0, Math.min(totalPages - 1, Number(page) || 0));
    const start = p * TAG_PAGE_SIZE;

    return {
        p,
        totalPages,
        slice: tags.slice(start, start + TAG_PAGE_SIZE),
        all: tags,
    };
}

// ====== UI builders ======
function flowStatusEmbed({ mode, step, postName = '', visited = null }) {
    const modeLabel = mode === 'edit' ? '✏ 編集中' : '➕ 記録作成中';
    const nameLine = postName ? `\n🍽 ${postName}` : '';
    const visitedLine =
        visited == null ? '' : `\n${visited ? '✅ 行った' : '📝 行きたい'}`;

    return new EmbedBuilder()
        .setTitle(modeLabel)
        .setDescription(`現在の入力: ${step}${nameLine}${visitedLine}`);
}

function buildCreatePanelEmbed(d = {}) {
    const lines = [
        `🍽 お店の名前: ${d.name?.trim() ? d.name : '(未入力)'}`,
        `✅ 訪問状態: ${d.visited == null ? '未選択' : (d.visited ? '行った' : '行きたい')}`,
        ...(d.visited === true
            ? [`⭐ 評価: ${d.rating ? stars(d.rating) : '未選択（必須）'}`]
            : []),
        `💬 コメント: ${d.comment?.trim() ? d.comment : '(なし)'}`,
        `🗾 都道府県: ${d.prefecture || '(未設定)'}`,
        ...(d.visited === true
            ? [`📅 行った日付: ${d.visitedDate || '(未入力)'}`]
            : []),
        `🏷 タグ: ${d.tags?.length ? tagString(d.tags) : '(なし)'}`,
        `🔗 Webサイト: ${d.url || '(なし)'}`,
        `📍 場所: ${d.mapUrl || '(なし)'}`,
        `📷 写真: 登録後に追加`,
        '',
        '項目を好きな順番で入力して「作成」を押してください',
    ];

    return new EmbedBuilder()
        .setTitle('➕ 記録作成中')
        .setDescription(lines.join('\n'));
}

function buildEditPanelEmbed(d = {}) {
    const lines = [
        `🍽 お店の名前: ${d.name?.trim() ? d.name : '(未入力)'}`,
        `✅ 訪問状態: ${d.visited == null ? '未選択' : (d.visited ? '行った' : '行きたい')}`,
        ...(d.visited === true
            ? [`⭐ 評価: ${d.rating ? stars(d.rating) : '未選択（必須）'}`]
            : []),
        `💬 コメント: ${d.comment?.trim() ? d.comment : '(なし)'}`,
        `🗾 都道府県: ${d.prefecture || '(未設定)'}`,
        ...(d.visited === true
            ? [`📅 行った日付: ${d.visitedDate || '(未入力)'}`]
            : []),
        `🏷 タグ: ${d.tags?.length ? tagString(d.tags) : '(なし)'}`,
        `🔗 Webサイト: ${d.url || '(なし)'}`,
        `📍 場所: ${d.mapUrl || '(なし)'}`,
        `📷 写真: 写真管理から編集`,
        '',
        '項目を好きな順番で編集して「更新」を押してください',
    ];

    return new EmbedBuilder()
        .setTitle(`✏ 編集中${d.name ? `: ${d.name}` : ''}`)
        .setDescription(lines.join('\n'));
}

function createPanelComponents(guildId, userId, d = {}) {
    const row1 = [
        new ButtonBuilder()
            .setCustomId(`create:setName:${guildId}:${userId}`)
            .setLabel('お店の名前')
            .setStyle(ButtonStyle.Secondary),

        new ButtonBuilder()
            .setCustomId(`create:setVisit:${guildId}:${userId}`)
            .setLabel('訪問状態')
            .setStyle(ButtonStyle.Secondary),
    ];

    if (d.visited === true) {
        row1.push(
            new ButtonBuilder()
                .setCustomId(`create:setRating:${guildId}:${userId}`)
                .setLabel('評価')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`create:setDate:${guildId}:${userId}`)
                .setLabel('行った日付')
                .setStyle(ButtonStyle.Secondary),
        );
    }

    row1.push(
        new ButtonBuilder()
            .setCustomId(`create:setPref:${guildId}:${userId}`)
            .setLabel('都道府県')
            .setStyle(ButtonStyle.Secondary),
    );

    return [
        new ActionRowBuilder().addComponents(...row1),

        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`create:setComment:${guildId}:${userId}`)
                .setLabel('コメント')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`create:setTags:${guildId}:${userId}`)
                .setLabel('タグ')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`create:setUrl:${guildId}:${userId}`)
                .setLabel('Webサイト')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`create:setMap:${guildId}:${userId}`)
                .setLabel('場所')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`create:photos:${guildId}:${userId}`)
                .setLabel('写真管理')
                .setStyle(ButtonStyle.Secondary),
        ),

        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`create:submit:${guildId}:${userId}`)
                .setLabel('作成')
                .setStyle(ButtonStyle.Primary),

            new ButtonBuilder()
                .setCustomId(`create:reset:${guildId}:${userId}`)
                .setLabel('リセット')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`create:home:${guildId}:${userId}`)
                .setLabel('🏠 ホーム')
                .setStyle(ButtonStyle.Secondary),
        ),
    ];
}

function editPanelComponents(guildId, userId, d = {}) {
    const row1 = [
        new ButtonBuilder()
            .setCustomId(`edit:setName:${guildId}:${userId}`)
            .setLabel('お店の名前')
            .setStyle(ButtonStyle.Secondary),

        new ButtonBuilder()
            .setCustomId(`edit:setVisit:${guildId}:${userId}`)
            .setLabel('訪問状態')
            .setStyle(ButtonStyle.Secondary),
    ];

    if (d.visited === true) {
        row1.push(
            new ButtonBuilder()
                .setCustomId(`edit:setRating:${guildId}:${userId}`)
                .setLabel('評価')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`edit:setDate:${guildId}:${userId}`)
                .setLabel('行った日付')
                .setStyle(ButtonStyle.Secondary),
        );
    }

    row1.push(
        new ButtonBuilder()
            .setCustomId(`edit:setPref:${guildId}:${userId}`)
            .setLabel('都道府県')
            .setStyle(ButtonStyle.Secondary),
    );

    return [
        new ActionRowBuilder().addComponents(...row1),

        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`edit:setComment:${guildId}:${userId}`)
                .setLabel('コメント')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`edit:setTags:${guildId}:${userId}`)
                .setLabel('タグ')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`edit:setUrl:${guildId}:${userId}`)
                .setLabel('Webサイト')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`edit:setMap:${guildId}:${userId}`)
                .setLabel('場所')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`edit:photos:${guildId}:${userId}`)
                .setLabel('写真管理')
                .setStyle(ButtonStyle.Secondary),
        ),

        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`edit:submit:${guildId}:${userId}`)
                .setLabel('更新')
                .setStyle(ButtonStyle.Primary),

            new ButtonBuilder()
                .setCustomId(`edit:back:${guildId}:${userId}`)
                .setLabel('戻る')
                .setStyle(ButtonStyle.Secondary),
        ),
    ];
}

async function renderCreatePanel(interaction, guildId, userId, { update = true } = {}) {
    const k = keyOf(guildId, userId);

    const d = draftRating.get(k) ?? {
        mode: 'create',
        postId: null,
        visited: null,
        rating: null,
        comment: '',
        prefecture: '',
        visitedDate: '',
        tags: [],
        url: '',
        mapUrl: '',
        name: '',
        channelId: interaction.channelId,
    };

    draftRating.set(k, d);

    const payload = {
        content: '',
        embeds: [buildCreatePanelEmbed(d)],
        components: createPanelComponents(guildId, userId, d),
    };

    if (update) {
        await interaction.update(payload);

        createPanelPromptRef.set(k, {
            webhook: interaction.webhook,
            messageId: interaction.message?.id,
        });

        return;
    }

    await interaction.reply({
        ephemeral: true,
        ...payload,
    });

    const msg = await interaction.fetchReply().catch(() => null);
    if (msg?.id) {
        addUiMessageId(guildId, userId, msg.id);
        createPanelPromptRef.set(k, {
            webhook: interaction.webhook,
            messageId: msg.id,
        });
    }
}

async function renderEditPanel(interaction, guildId, userId, { update = true } = {}) {
    const k = keyOf(guildId, userId);
    const d = draftRating.get(k);

    if (!d || d.mode !== 'edit') {
        return interaction.reply({
            ephemeral: true,
            content: '編集状態がありません',
        });
    }

    const payload = {
        content: '',
        embeds: [buildEditPanelEmbed(d)],
        components: editPanelComponents(guildId, userId, d),
    };

    if (update) {
        await interaction.update(payload);

        editPanelPromptRef.set(k, {
            webhook: interaction.webhook,
            messageId: interaction.message?.id,
        });
        return;
    }

    await interaction.reply({
        ephemeral: true,
        ...payload,
    });

    const msg = await interaction.fetchReply().catch(() => null);
    if (msg?.id) {
        addUiMessageId(guildId, userId, msg.id);
        editPanelPromptRef.set(k, {
            webhook: interaction.webhook,
            messageId: msg.id,
        });
    }
}

async function rerenderCreatePanelFromRef(interaction, guildId, userId) {
    const k = keyOf(guildId, userId);
    const d = draftRating.get(k);

    if (!d) {
        return interaction.reply({
            ephemeral: true,
            content: '新規登録状態がありません',
        });
    }

    const ref = createPanelPromptRef.get(k);

    const payload = {
        content: '',
        embeds: [buildCreatePanelEmbed(d)],
        components: createPanelComponents(guildId, userId, d),
    };

    const updated = await editPromptRef(ref, payload);

    if (updated) {
        try {
            if (!interaction.replied && !interaction.deferred) {
                const okMsg = await interaction.reply({
                    ephemeral: true,
                    content: '✅ 更新しました',
                    fetchReply: true,
                });

                setTimeout(() => {
                    okMsg.delete().catch(() => { });
                }, 2000);
            }
        } catch { }
        return true;
    }
    return false;
}

async function rerenderEditPanelFromRef(interaction, guildId, userId) {
    const k = keyOf(guildId, userId);
    const d = draftRating.get(k);

    if (!d || d.mode !== 'edit') {
        return interaction.reply({
            ephemeral: true,
            content: '編集状態がありません',
        });
    }

    const ref = editPanelPromptRef.get(k);

    const payload = {
        content: '',
        embeds: [buildEditPanelEmbed(d)],
        components: editPanelComponents(guildId, userId, d),
    };

    const updated = await editPromptRef(ref, payload);

    if (updated) {
        try {
            if (!interaction.replied && !interaction.deferred) {
                const okMsg = await interaction.reply({
                    ephemeral: true,
                    content: '✅ 更新しました',
                    fetchReply: true,
                });

                setTimeout(() => {
                    okMsg.delete().catch(() => { });
                }, 2000);
            }
        } catch { }
        return true;
    }

    return false;
}

function photoWaitingEmbed(name) {
    return new EmbedBuilder()
        .setTitle('📷 写真追加')
        .setDescription(
            `**${name}** を登録しました\n\n` +
            'このチャンネルに写真を送信してください\n' +
            '送信した画像をすべてこの記録に追加します\n' +
            '投稿後、Botが元メッセージを削除します'
        );
}

function photoCreateWaitingEmbed(name) {
    return new EmbedBuilder()
        .setTitle('✅ 登録完了 / 📷 写真追加')
        .setDescription(
            `**${name}** を登録しました\n\n` +
            'このチャンネルに写真を送信してください\n' +
            '送信した画像をすべてこの記録に追加します\n' +
            '投稿後、Botが元メッセージを削除します'
        );
}

function photoAddWaitingEmbed(name) {
    return new EmbedBuilder()
        .setTitle('📷 写真追加')
        .setDescription(
            `**${name}** に写真を追加します\n` +
            'このチャンネルに写真を送信してください\n' +
            '送信した画像をすべて追加します。投稿後、Botが元メッセージを削除します'
        );
}

function photoWaitingComponents(guildId, userId) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId('photo:waiting')
                .setLabel('写真を送信待ち')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(true),

            new ButtonBuilder()
                .setCustomId(`photo:skip:${guildId}:${userId}`)
                .setLabel('送信しない')
                .setStyle(ButtonStyle.Secondary)
        )
    ];
}

function photoWaitingComponentsForCreate(guildId, userId) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId('photo:waiting')
                .setLabel('写真を送信待ち')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(true),

            new ButtonBuilder()
                .setCustomId(`photo:skip:${guildId}:${userId}`)
                .setLabel('送信しない')
                .setStyle(ButtonStyle.Secondary)
        ),
    ];
}

function buildVisitedDateModal(gid, ownerId, mode, postId = '', currentValue = '') {

    const todayStr = todayYMD();

    return new ModalBuilder()
        .setCustomId(`modalVisitedDate:${gid}:${ownerId}:${mode}:${postId}`)
        .setTitle('📅 行った日付')
        .addComponents(
            new ActionRowBuilder().addComponents(
                new TextInputBuilder()
                    .setCustomId('visitedDate')
                    .setLabel('行った日付')
                    .setStyle(TextInputStyle.Short)
                    .setRequired(false)
                    .setPlaceholder(`${todayStr} または ${todayStr.replace(/\//g, '-')}`)
                    .setValue(currentValue || todayStr)
            )
        );
}

function buildCommentModal(gid, ownerId, currentValue = '') {
    return new ModalBuilder()
        .setCustomId(`modalCreateComment:${gid}:${ownerId}`)
        .setTitle('💬 コメント')
        .addComponents(
            new ActionRowBuilder().addComponents(
                new TextInputBuilder()
                    .setCustomId('comment')
                    .setLabel('コメント')
                    .setPlaceholder('500文字以内で入力してください')
                    .setStyle(TextInputStyle.Paragraph)
                    .setMaxLength(500)
                    .setRequired(false)
                    .setValue(currentValue ?? '')
            )
        );
}

function buildTagsModal(gid, ownerId, currentValue = '') {
    return new ModalBuilder()
        .setCustomId(`modalCreateTags:${gid}:${ownerId}`)
        .setTitle('🏷 タグ')
        .addComponents(
            new ActionRowBuilder().addComponents(
                new TextInputBuilder()
                    .setCustomId('tags')
                    .setLabel('タグ（カンマ区切り）')
                    .setStyle(TextInputStyle.Short)
                    .setRequired(false)
                    .setPlaceholder('ラーメン, デート, 深夜')
                    .setValue(currentValue ?? '')
            )
        );
}

function buildUrlModal(gid, ownerId, currentValue = '') {
    return new ModalBuilder()
        .setCustomId(`modalCreateUrl:${gid}:${ownerId}`)
        .setTitle('🔗 Webサイト')
        .addComponents(
            new ActionRowBuilder().addComponents(
                new TextInputBuilder()
                    .setCustomId('url')
                    .setLabel('Webサイト')
                    .setStyle(TextInputStyle.Short)
                    .setRequired(false)
                    .setValue(currentValue ?? '')
            )
        );
}

function buildMapModal(gid, ownerId, currentValue = '') {
    return new ModalBuilder()
        .setCustomId(`modalCreateMap:${gid}:${ownerId}`)
        .setTitle('📍 場所')
        .addComponents(
            new ActionRowBuilder().addComponents(
                new TextInputBuilder()
                    .setCustomId('mapUrl')
                    .setLabel('GoogleMapリンク')
                    .setStyle(TextInputStyle.Short)
                    .setRequired(false)
                    .setValue(currentValue ?? '')
            )
        );
}

function buildNameModal(gid, ownerId, currentValue = '') {
    return new ModalBuilder()
        .setCustomId(`modalCreateName:${gid}:${ownerId}`)
        .setTitle('🍽 お店の名前')
        .addComponents(
            new ActionRowBuilder().addComponents(
                new TextInputBuilder()
                    .setCustomId('name')
                    .setLabel('お店の名前')
                    .setStyle(TextInputStyle.Short)
                    .setRequired(true)
                    .setValue(currentValue ?? '')
            )
        );
}

function buildEditNameModal(gid, ownerId, currentValue = '') {
    return new ModalBuilder()
        .setCustomId(`modalEditName:${gid}:${ownerId}`)
        .setTitle('🍽 お店の名前')
        .addComponents(
            new ActionRowBuilder().addComponents(
                new TextInputBuilder()
                    .setCustomId('name')
                    .setLabel('お店の名前')
                    .setStyle(TextInputStyle.Short)
                    .setRequired(true)
                    .setValue(currentValue ?? '')
            )
        );
}

function buildEditCommentModal(gid, ownerId, currentValue = '') {
    return new ModalBuilder()
        .setCustomId(`modalEditComment:${gid}:${ownerId}`)
        .setTitle('💬 コメント')
        .addComponents(
            new ActionRowBuilder().addComponents(
                new TextInputBuilder()
                    .setCustomId('comment')
                    .setLabel('コメント')
                    .setPlaceholder('500文字以内で入力してください')
                    .setStyle(TextInputStyle.Paragraph)
                    .setMaxLength(500)
                    .setRequired(false)
                    .setValue(currentValue ?? '')
            )
        );
}

function buildEditTagsModal(gid, ownerId, currentValue = '') {
    return new ModalBuilder()
        .setCustomId(`modalEditTags:${gid}:${ownerId}`)
        .setTitle('🏷 タグ')
        .addComponents(
            new ActionRowBuilder().addComponents(
                new TextInputBuilder()
                    .setCustomId('tags')
                    .setLabel('タグ（カンマ区切り）')
                    .setStyle(TextInputStyle.Short)
                    .setRequired(false)
                    .setPlaceholder('ラーメン, デート, 深夜')
                    .setValue(currentValue ?? '')
            )
        );
}

function buildEditUrlModal(gid, ownerId, currentValue = '') {
    return new ModalBuilder()
        .setCustomId(`modalEditUrl:${gid}:${ownerId}`)
        .setTitle('🔗 Webサイト')
        .addComponents(
            new ActionRowBuilder().addComponents(
                new TextInputBuilder()
                    .setCustomId('url')
                    .setLabel('Webサイト')
                    .setStyle(TextInputStyle.Short)
                    .setRequired(false)
                    .setValue(currentValue ?? '')
            )
        );
}

function buildEditMapModal(gid, ownerId, currentValue = '') {
    return new ModalBuilder()
        .setCustomId(`modalEditMap:${gid}:${ownerId}`)
        .setTitle('📍 場所')
        .addComponents(
            new ActionRowBuilder().addComponents(
                new TextInputBuilder()
                    .setCustomId('mapUrl')
                    .setLabel('GoogleMapリンク')
                    .setStyle(TextInputStyle.Short)
                    .setRequired(false)
                    .setValue(currentValue ?? '')
            )
        );
}

function homeComponents() {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId('home:create').setLabel('➕ 記録する').setStyle(ButtonStyle.Primary),
            new ButtonBuilder().setCustomId('home:search').setLabel('🔎 検索する').setStyle(ButtonStyle.Secondary),
            new ButtonBuilder().setCustomId('home:mine').setLabel('📚 自分の記録').setStyle(ButtonStyle.Secondary)
        ),
    ];
}

function visitRow(prefix, guildId, userId, postId = '') {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`${prefix}:visited:${guildId}:${userId}:${postId}`)
                .setLabel('✅ 行った')
                .setStyle(ButtonStyle.Secondary),
            new ButtonBuilder()
                .setCustomId(`${prefix}:planned:${guildId}:${userId}:${postId}`)
                .setLabel('📝 行きたい')
                .setStyle(ButtonStyle.Secondary)
        ),
    ];
}

function ratingRow(prefix, guildId, userId, postId = '') {
    const row = new ActionRowBuilder();
    for (let i = 1; i <= 5; i++) {
        row.addComponents(
            new ButtonBuilder()
                .setCustomId(`${prefix}:${i}:${guildId}:${userId}:${postId}`)
                .setLabel('⭐'.repeat(i))
                .setStyle(ButtonStyle.Primary)
        );
    }
    return [row];
}

function prefPickComponents(mode, guildId, userId, page = 0) {
    const { p, totalPages, slice } = prefSlice(page);

    const select = new StringSelectMenuBuilder()
        .setCustomId(`${mode}:prefPick:${guildId}:${userId}:${p}`)
        .setPlaceholder(`都道府県を選択してください（任意） ${p + 1}/${totalPages}`)
        .setMinValues(0)
        .setMaxValues(1)
        .addOptions(slice.map(x => ({ label: x, value: x })));

    const nav = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId(`${mode}:prefPagePrev:${guildId}:${userId}:${p}`)
            .setLabel('◀ 前へ')
            .setStyle(ButtonStyle.Secondary)
            .setDisabled(p <= 0),

        new ButtonBuilder()
            .setCustomId(`${mode}:prefPageNext:${guildId}:${userId}:${p}`)
            .setLabel('次へ ▶')
            .setStyle(ButtonStyle.Secondary)
            .setDisabled(p >= totalPages - 1),
    );

    return [
        new ActionRowBuilder().addComponents(select),
        nav,
    ];
}

function searchPanelEmbed(state) {
    const userLabel = state.userIdFilter?.length
        ? state.userIdFilter.map(id => `<@${id}>`).join(' ')
        : '指定なし';

    const prefLabel = state.prefectureFilters?.length
        ? state.prefectureFilters.map(x => `#${x}`).join(' ')
        : '指定なし';

    const tagLabel = state.tagFilters?.length
        ? state.tagFilters.map(x => `#${x}`).join(' ')
        : '指定なし';

    const keywordLabel = state.keyword ? `「${state.keyword}」` : '指定なし';
    const ratingLabel = state.ratingFilters?.length
        ? state.ratingFilters.map(r => `⭐${r}`).join(' ')
        : '指定なし';

    return new EmbedBuilder()
        .setTitle('🔎 検索条件')
        .setDescription(
            `👤 登録者: ${userLabel}\n` +
            `🗾 都道府県: ${prefLabel}\n` +
            `🏷 タグ: ${tagLabel}\n` +
            `🔤 キーワード: ${keywordLabel}\n` +
            `⭐ 評価: ${ratingLabel}\n\n` +
            '条件を設定して「検索実行」を押してください'
        );
}

function searchPanelComponents(guildId, userId, st = {}) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`search:setUser:${guildId}:${userId}`)
                .setLabel('👤 登録者')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`search:setPref:${guildId}:${userId}`)
                .setLabel('🗾 都道府県')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`search:setTag:${guildId}:${userId}`)
                .setLabel('🏷 タグ')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`search:setText:${guildId}:${userId}`)
                .setLabel('🔤 キーワード')
                .setStyle(ButtonStyle.Secondary)
        ),
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`search:setRating:${guildId}:${userId}:1`)
                .setLabel(`${st?.ratingFilters?.includes(1) ? '☑' : '☐'} ⭐1`)
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`search:setRating:${guildId}:${userId}:2`)
                .setLabel(`${st?.ratingFilters?.includes(2) ? '☑' : '☐'} ⭐2`)
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`search:setRating:${guildId}:${userId}:3`)
                .setLabel(`${st?.ratingFilters?.includes(3) ? '☑' : '☐'} ⭐3`)
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`search:setRating:${guildId}:${userId}:4`)
                .setLabel(`${st?.ratingFilters?.includes(4) ? '☑' : '☐'} ⭐4`)
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`search:setRating:${guildId}:${userId}:5`)
                .setLabel(`${st?.ratingFilters?.includes(5) ? '☑' : '☐'} ⭐5`)
                .setStyle(ButtonStyle.Secondary)
        ),
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`search:run:${guildId}:${userId}`)
                .setLabel('✅ 検索実行')
                .setStyle(ButtonStyle.Primary),

            new ButtonBuilder()
                .setCustomId(`search:clear:${guildId}:${userId}`)
                .setLabel('🧹 条件クリア')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`search:back:${guildId}:${userId}`)
                .setLabel('🏠 ホーム')
                .setStyle(ButtonStyle.Secondary)
        ),
    ];
}

function photoManagerComponents(guildId, userId, postId, total = 0) {
    const hasAny = total > 0;

    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`ph:prev:${guildId}:${userId}:${postId}`)
                .setLabel('◀')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(total <= 1),

            new ButtonBuilder()
                .setCustomId(`ph:next:${guildId}:${userId}:${postId}`)
                .setLabel('▶')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(total <= 1),

            new ButtonBuilder()
                .setCustomId(`ph:add:${guildId}:${userId}:${postId}`)
                .setLabel('➕ 追加')
                .setStyle(ButtonStyle.Primary)
        ),
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`ph:del:${guildId}:${userId}:${postId}`)
                .setLabel('🗑 この写真削除')
                .setStyle(ButtonStyle.Danger)
                .setDisabled(!hasAny),

            new ButtonBuilder()
                .setCustomId(`ph:delall:${guildId}:${userId}:${postId}`)
                .setLabel('🗑 すべて削除')
                .setStyle(ButtonStyle.Danger)
                .setDisabled(!hasAny),

            new ButtonBuilder()
                .setCustomId(`ph:back:${guildId}:${userId}:${postId}`)
                .setLabel('🔙 戻る')
                .setStyle(ButtonStyle.Secondary)
        ),
    ];
}

function confirmComponents(kind, guildId, userId, postId, extra = '', hasImages = true) {
    const rows = [];

    if (kind === 'deletePost' || kind === 'deleteAllPhotos') {
        rows.push(
            new ActionRowBuilder().addComponents(
                new ButtonBuilder()
                    .setCustomId(`delimg:prev:${guildId}:${userId}:${postId}:${extra}:${kind}`)
                    .setLabel('◀')
                    .setStyle(ButtonStyle.Secondary)
                    .setDisabled(!hasImages),

                new ButtonBuilder()
                    .setCustomId(`delimg:next:${guildId}:${userId}:${postId}:${extra}:${kind}`)
                    .setLabel('▶')
                    .setStyle(ButtonStyle.Secondary)
                    .setDisabled(!hasImages)
            )
        );
    }

    rows.push(
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`confirm:yes:${kind}:${guildId}:${userId}:${postId}:${extra}`)
                .setLabel('はい')
                .setStyle(ButtonStyle.Danger),
            new ButtonBuilder()
                .setCustomId(`confirm:no:${kind}:${guildId}:${userId}:${postId}:${extra}`)
                .setLabel('いいえ')
                .setStyle(ButtonStyle.Secondary)
        )
    );

    return rows;
}

function confirmEmbed(title, message) {
    return new EmbedBuilder()
        .setTitle(title)
        .setDescription(message);
}

function confirmPhotoDeleteEmbed(post, idx) {
    const urls = imageUrls(post);
    const safeIdx = Math.max(0, Math.min(urls.length - 1, Number(idx) || 0));

    const e = new EmbedBuilder()
        .setTitle('⚠ 写真を削除')
        .setDescription(
            `**${post.name}** のこの写真を削除しますか？\n` +
            (urls.length ? `写真 ${safeIdx + 1}/${urls.length}` : '')
        );

    if (urls.length) {
        e.setImage(urls[safeIdx]);
    }

    return e;
}

function confirmDeletePostEmbed(post, { imageIndex = null } = {}) {
    const urls = imageUrls(post);
    const idx = Math.max(0, Math.min(urls.length - 1, Number(imageIndex) || 0));

    const e = buildPostEmbedForView(post, { imageIndex: idx });

    e.setTitle(`⚠ 店情報を削除: ${post.name}`);

    e.spliceFields(0, 0, {
        name: '確認',
        value: 'この記録と写真をすべて削除します。本当に削除しますか？',
    });

    if (urls.length) {
        e.addFields({
            name: '📷 写真',
            value: `${urls.length}枚登録されています`,
        });
    }

    return e;
}

function confirmDeleteAllPhotosEmbed(post) {
    const urls = imageUrls(post);

    const e = new EmbedBuilder()
        .setTitle('⚠ 写真をすべて削除')
        .setDescription(`**${post.name}** の写真をすべて削除しますか？`);

    if (urls.length) {
        e.setImage(urls[Math.max(0, urls.length - 1)]);
    }

    return e;
}

function mineListComponents(guildId, userId, page, hasPrev, hasNext, options, st) {
    const filter = st?.visitFilter ?? 'all';

    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`mine:filter:all:${guildId}:${userId}`)
                .setLabel(`${filter === 'all' ? '◉' : '○'} すべて`)
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`mine:filter:visited:${guildId}:${userId}`)
                .setLabel(`${filter === 'visited' ? '◉' : '○'} 行った`)
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`mine:filter:planned:${guildId}:${userId}`)
                .setLabel(`${filter === 'planned' ? '◉' : '○'} 行きたい`)
                .setStyle(ButtonStyle.Secondary),
        ),
        new ActionRowBuilder().addComponents(
            new StringSelectMenuBuilder()
                .setCustomId(`mine:pick:${guildId}:${userId}:${page}`)
                .setPlaceholder('お店を選んで詳細へ')
                .setMinValues(1)
                .setMaxValues(1)
                .addOptions(options?.length ? options : [{ label: '(なし)', value: 'none', description: '選択できません' }])
        ),
        new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId(`mine:prev:${guildId}:${userId}`).setLabel('◀').setStyle(ButtonStyle.Secondary).setDisabled(!hasPrev),
            new ButtonBuilder().setCustomId(`mine:next:${guildId}:${userId}`).setLabel('▶').setStyle(ButtonStyle.Secondary).setDisabled(!hasNext),
            new ButtonBuilder().setCustomId(`mine:home:${guildId}:${userId}`).setLabel('🏠 ホーム').setStyle(ButtonStyle.Secondary)
        ),
    ];
}

function searchResultListComponents(guildId, userId, page, hasPrev, hasNext, options) {
    return [
        new ActionRowBuilder().addComponents(
            new StringSelectMenuBuilder()
                .setCustomId(`search:pick:${guildId}:${userId}:${page}`)
                .setPlaceholder('お店を選んで詳細へ')
                .setMinValues(1)
                .setMaxValues(1)
                .addOptions(options?.length ? options : [{ label: '(なし)', value: 'none', description: '選択できません' }])
        ),
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`search:listPrev:${guildId}:${userId}`)
                .setLabel('◀')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(!hasPrev),

            new ButtonBuilder()
                .setCustomId(`search:listNext:${guildId}:${userId}`)
                .setLabel('▶')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(!hasNext),

            new ButtonBuilder()
                .setCustomId(`search:backToPanel:${guildId}:${userId}`)
                .setLabel('🔎 条件へ戻る')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`search:home:${guildId}:${userId}`)
                .setLabel('🏠 ホーム')
                .setStyle(ButtonStyle.Secondary)
        ),
    ];
}

// ====== コマンド登録 ======
async function registerCommands() {
    const cmd = new SlashCommandBuilder()
        .setName('gourmet')
        .setDescription('グルメ記録を開く');

    const rest = new REST({ version: '10' }).setToken(TOKEN);

    const guilds = await client.guilds.fetch();

    for (const [, g] of guilds) {
        await rest.put(
            Routes.applicationGuildCommands(client.user.id, g.id),
            { body: [cmd.toJSON()] }
        );
    }

    console.log('Commands registered to all current guilds');
}

// ====== 画面ヘルパ ======
function homeEmbed() {
    return new EmbedBuilder()
        .setTitle('🍽 グルメ記録')
        .setDescription('操作を選択してください');
}

async function updateLike(interaction, payload) {
    if (interaction.deferred || interaction.replied) {
        return interaction.editReply(payload);
    }
    return interaction.update(payload);
}

async function renderMineList(interaction, guildId, userId, { update = false } = {}) {
    await ensureCacheLoadedForGuild(interaction.guild);
    const cache = getGuildCache(guildId);

    const k = keyOf(guildId, userId);
    const st = mineState.get(k);

    if (!st?.results?.length) {
        const e = new EmbedBuilder()
            .setTitle('📚 自分の記録')
            .setDescription('(まだありません)');

        if (update) {
            return updateLike(interaction, {
                embeds: [e],
                components: homeComponents(),
            });
        }

        await interaction.reply({
            ephemeral: true,
            embeds: [e],
            components: homeComponents(),
        });
        await rememberUiReply(interaction, guildId, userId);
        return;
    }

    const filteredIds = st.results.filter(pid => {
        const p = cache.get(pid);
        if (!p) return false;
        return visitFilterMatch(st, p);
    });

    const pageSize = 9;
    let page = Math.max(0, Number(st.page) || 0);
    const maxPage = Math.max(0, Math.ceil(filteredIds.length / pageSize) - 1);
    if (page > maxPage) page = maxPage;
    st.page = page;
    mineState.set(k, st);

    const start = page * pageSize;
    const slice = filteredIds
        .slice(start, start + pageSize)
        .map(pid => cache.get(pid))
        .filter(Boolean);

    const listHeader = new EmbedBuilder()
        .setTitle('📚 自分の記録')
        .setDescription(`表示 ${start + 1}-${start + slice.length} / ${filteredIds.length} 件`);

    const options = slice.slice(0, 25).map(p => ({
        label: (p.name ?? '').slice(0, 100),
        description: `${visitLabel(p)} / ${p.prefecture || '未設定'}`.slice(0, 100),
        value: p.id,
    }));

    const hasPrev = page > 0;
    const hasNext = start + pageSize < filteredIds.length;

    const comps = mineListComponents(guildId, userId, page, hasPrev, hasNext, options, st);
    const embeds = [listHeader, ...slice.map(buildCardEmbed)];

    if (update) {
        return updateLike(interaction, {
            embeds,
            components: comps,
        });
    }

    await interaction.reply({
        ephemeral: true,
        embeds,
        components: comps,
    });
    await rememberUiReply(interaction, guildId, userId);
}

async function renderSearchResultList(interaction, guildId, userId, { update = false } = {}) {
    const k = keyOf(guildId, userId);
    const st = searchState.get(k);

    if (!st?.results?.length) {
        const panel = searchState.get(k) ?? {
            userIdFilter: null,
            prefectureFilters: [],
            keyword: '',
            ratingFilters: [],
            results: [],
            page: 0
        };

        const payload = {
            embeds: [searchPanelEmbed(panel)],
            components: searchPanelComponents(guildId, userId, panel),
        };

        if (update) return updateLike(interaction, payload);

        await interaction.reply({ ephemeral: true, ...payload });
        await rememberUiReply(interaction, guildId, userId);
        return;
    }

    await ensureCacheLoadedForGuild(interaction.guild);
    const cache = getGuildCache(guildId);

    const pageSize = 9;
    let page = Math.max(0, Number(st.page) || 0);
    const maxPage = Math.max(0, Math.ceil(st.results.length / pageSize) - 1);
    if (page > maxPage) page = maxPage;
    st.page = page;
    searchState.set(k, st);

    const start = page * pageSize;
    const slice = st.results
        .slice(start, start + pageSize)
        .map(pid => cache.get(pid))
        .filter(Boolean);

    const header = new EmbedBuilder()
        .setTitle('🔎 検索結果一覧')
        .setDescription(`表示 ${st.results.length ? start + 1 : 0}-${start + slice.length} / ${st.results.length} 件`);

    const options = slice.slice(0, 25).map(p => ({
        label: (p.name ?? '').slice(0, 100),
        description: `${visitLabel(p)} / ${p.prefecture || '未設定'}`.slice(0, 100),
        value: p.id,
    }));

    const hasPrev = page > 0;
    const hasNext = start + pageSize < st.results.length;

    const embeds = [header, ...slice.map(buildSearchCardEmbed)];
    const components = searchResultListComponents(guildId, userId, page, hasPrev, hasNext, options);

    const payload = { embeds, components };

    if (update) return updateLike(interaction, payload);

    await interaction.reply({ ephemeral: true, ...payload });
    await rememberUiReply(interaction, guildId, userId);
}

function detailActionComponents(
    guildId,
    userId,
    postId,
    { fromMine = false, canEditThis = true, total = 1, forceHomeBack = false, hasPhotos = false } = {}
) {
    const row1 = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId(`res:prev:${guildId}:${userId}:${postId}`)
            .setLabel('◀')
            .setStyle(ButtonStyle.Secondary)
            .setDisabled(!hasPhotos),

        new ButtonBuilder()
            .setCustomId(`res:next:${guildId}:${userId}:${postId}`)
            .setLabel('▶')
            .setStyle(ButtonStyle.Secondary)
            .setDisabled(!hasPhotos),

        new ButtonBuilder()
            .setCustomId(`res:share:${guildId}:${userId}:${postId}`)
            .setLabel('📤 このチャンネルに送信')
            .setStyle(ButtonStyle.Primary)
    );

    const backCustomId = forceHomeBack
        ? `mine:home:${guildId}:${userId}`
        : (fromMine ? `mine:back:${guildId}:${userId}` : `search:listBack:${guildId}:${userId}`);

    const backLabel = forceHomeBack
        ? '🏠 ホーム'
        : '🔙 戻る';

    const row2Buttons = [];

    if (canEditThis) {
        row2Buttons.push(
            new ButtonBuilder()
                .setCustomId(`res:edit:${guildId}:${userId}:${postId}`)
                .setLabel('✏ 編集')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`res:photos:${guildId}:${userId}:${postId}`)
                .setLabel('🖼 写真管理')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`res:delete:${guildId}:${userId}:${postId}`)
                .setLabel('🗑 削除')
                .setStyle(ButtonStyle.Danger)
        );
    }

    row2Buttons.push(
        new ButtonBuilder()
            .setCustomId(backCustomId)
            .setLabel(backLabel)
            .setStyle(ButtonStyle.Secondary)
    );

    const row2 = new ActionRowBuilder().addComponents(...row2Buttons);

    return [row1, row2];
}

function cameFromMine(k, postId, mineState) {
    const mine = mineState.get(k);
    return !!mine?.results?.includes(postId);
}

function renderDetail(
    interaction,
    { post, guildId, userId, fromMine, total = 1, forceHomeBack = false, notice = null }
) {
    setDetailNavState(guildId, userId, post.id, {
        fromMine,
        forceHomeBack,
    });

    const k = keyOf(guildId, userId);
    const urls = imageUrls(post);

    let idx = 0;
    const pv = detailPhotoView.get(k);

    if (pv?.postId === post.id) {
        idx = Math.max(0, Math.min(urls.length - 1, Number(pv.idx) || 0));
    } else {
        idx = Math.max(0, urls.length - 1); // 初回は最後の写真
        detailPhotoView.set(k, { postId: post.id, idx });
    }

    const detail = buildPostEmbedForView(post, { imageIndex: idx, notice });
    detail.setTitle(`📄 詳細  ${post.name}`.trim());

    const components = detailActionComponents(guildId, userId, post.id, {
        fromMine,
        canEditThis: canEdit(interaction, post),
        total,
        forceHomeBack,
        hasPhotos: urls.length > 1,
    });

    return { detail, components };
}

// ====== interactions ======
client.on(Events.InteractionCreate, async interaction => {
    try {
        if (!interaction.guildId) return;
        const guildId = interaction.guildId;
        const userId = interaction.user.id;
        const k = keyOf(guildId, userId);
        const id = interaction.customId ?? '';

        // Slash command
        if (interaction.isChatInputCommand()) {
            if (interaction.commandName !== 'gourmet') return;

            await interaction.deferReply({ ephemeral: true });

            try {
                await ensureCacheLoadedForGuild(interaction.guild);
            } catch (e) {
                return interaction.editReply({
                    content: `エラー: ${e.message}`,
                    embeds: [],
                    components: [],
                });
            }

            await interaction.editReply({
                content: '',
                embeds: [homeEmbed()],
                components: homeComponents(),
            });

            await rememberUiReply(interaction, guildId, userId);
            return;
        }

        // Buttons
        if (interaction.isButton()) {
            if (id.startsWith('search:listPrev:') || id.startsWith('search:listNext:')) {
                const [, action, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const st = searchState.get(k);
                if (!st?.results?.length) {
                    return interaction.reply({ ephemeral: true, content: '検索結果がありません' });
                }

                const pageSize = 9;
                const maxPage = Math.max(0, Math.ceil(st.results.length / pageSize) - 1);

                st.page = Number(st.page) || 0;
                st.page += action === 'listPrev' ? -1 : 1;
                if (st.page < 0) st.page = 0;
                if (st.page > maxPage) st.page = maxPage;

                searchState.set(k, st);

                return renderSearchResultList(interaction, guildId, userId, { update: true });
            }

            if (id.startsWith('search:setTag:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                await ensureCacheLoadedForGuild(interaction.guild);
                const cache = getGuildCache(guildId);

                const st = searchState.get(k) ?? {
                    userIdFilter: null,
                    prefectureFilters: [],
                    tagFilters: [],
                    keyword: '',
                    ratingFilters: [],
                    results: [],
                    page: 0
                };

                const { p, totalPages, slice } = tagSlice(cache, 0);

                const select = new StringSelectMenuBuilder()
                    .setCustomId(`search:tagPick:${guildId}:${ownerId}:${p}`)
                    .setPlaceholder(`タグを選択してください（複数可） ${p + 1}/${totalPages}`)
                    .setMinValues(0)
                    .setMaxValues(slice.length || 1)
                    .addOptions(
                        (slice.length ? slice : ['(タグなし)']).map(x => ({
                            label: x,
                            value: x,
                            default: st.tagFilters?.includes(x) ?? false,
                        }))
                    );

                const nav = new ActionRowBuilder().addComponents(
                    new ButtonBuilder()
                        .setCustomId(`search:tagPagePrev:${guildId}:${ownerId}:${p}`)
                        .setLabel('◀ 前へ')
                        .setStyle(ButtonStyle.Secondary)
                        .setDisabled(p <= 0),

                    new ButtonBuilder()
                        .setCustomId(`search:tagPageNext:${guildId}:${ownerId}:${p}`)
                        .setLabel('次へ ▶')
                        .setStyle(ButtonStyle.Secondary)
                        .setDisabled(p >= totalPages - 1),

                    new ButtonBuilder()
                        .setCustomId(`search:tagPageClear:${guildId}:${ownerId}`)
                        .setLabel('解除')
                        .setStyle(ButtonStyle.Secondary),

                    new ButtonBuilder()
                        .setCustomId(`search:tagBack:${guildId}:${ownerId}`)
                        .setLabel('戻る')
                        .setStyle(ButtonStyle.Secondary),
                );

                return interaction.update({
                    content: 'タグを選択してください',
                    embeds: [],
                    components: [new ActionRowBuilder().addComponents(select), nav],
                });
            }

            if (id.startsWith('search:tagPagePrev:') || id.startsWith('search:tagPageNext:')) {
                const parts = id.split(':');
                const gid = parts[2];
                const ownerId = parts[3];
                const page = Number(parts[4] || 0);

                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                await ensureCacheLoadedForGuild(interaction.guild);
                const cache = getGuildCache(guildId);

                const next = id.includes('tagPageNext') ? page + 1 : page - 1;
                const { p, totalPages, slice } = tagSlice(cache, next);

                const st = searchState.get(k) ?? {
                    userIdFilter: null,
                    prefectureFilters: [],
                    tagFilters: [],
                    keyword: '',
                    ratingFilters: [],
                    results: [],
                    page: 0
                };

                if (!slice.length) {
                    return interaction.update({
                        content: '',
                        embeds: [searchPanelEmbed(st)],
                        components: searchPanelComponents(guildId, userId, st),
                    });
                }

                const select = new StringSelectMenuBuilder()
                    .setCustomId(`search:tagPick:${guildId}:${ownerId}:${p}`)
                    .setPlaceholder(`タグを選択してください（複数可） ${p + 1}/${totalPages}`)
                    .setMinValues(0)
                    .setMaxValues(slice.length)
                    .addOptions(
                        slice.map(x => ({
                            label: x,
                            value: x,
                            default: st.tagFilters?.includes(x) ?? false,
                        }))
                    );

                const nav = new ActionRowBuilder().addComponents(
                    new ButtonBuilder()
                        .setCustomId(`search:tagPagePrev:${guildId}:${ownerId}:${p}`)
                        .setLabel('◀ 前へ')
                        .setStyle(ButtonStyle.Secondary)
                        .setDisabled(p <= 0),

                    new ButtonBuilder()
                        .setCustomId(`search:tagPageNext:${guildId}:${ownerId}:${p}`)
                        .setLabel('次へ ▶')
                        .setStyle(ButtonStyle.Secondary)
                        .setDisabled(p >= totalPages - 1),

                    new ButtonBuilder()
                        .setCustomId(`search:tagPageClear:${guildId}:${ownerId}`)
                        .setLabel('解除')
                        .setStyle(ButtonStyle.Secondary),

                    new ButtonBuilder()
                        .setCustomId(`search:tagBack:${guildId}:${ownerId}`)
                        .setLabel('戻る')
                        .setStyle(ButtonStyle.Secondary),
                );

                return interaction.update({
                    content: 'タグを選択してください',
                    embeds: [],
                    components: [new ActionRowBuilder().addComponents(select), nav],
                });
            }

            if (id.startsWith('search:tagPageClear:')) {
                const parts = id.split(':');
                const gid = parts[2];
                const ownerId = parts[3];

                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const st = searchState.get(k) ?? {
                    userIdFilter: null,
                    prefectureFilters: [],
                    tagFilters: [],
                    keyword: '',
                    ratingFilters: [],
                    results: [],
                    page: 0
                };

                st.tagFilters = [];
                searchState.set(k, st);

                return interaction.update({
                    content: 'タグフィルタを解除しました',
                    embeds: [searchPanelEmbed(st)],
                    components: searchPanelComponents(guildId, userId, st),
                });
            }

            if (id.startsWith('search:tagBack:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const st = searchState.get(k) ?? {
                    userIdFilter: null,
                    prefectureFilters: [],
                    tagFilters: [],
                    keyword: '',
                    ratingFilters: [],
                    results: [],
                    page: 0
                };

                return interaction.update({
                    content: '',
                    embeds: [searchPanelEmbed(st)],
                    components: searchPanelComponents(guildId, userId, st),
                });
            }

            if (id.startsWith('photo:skip:')) {
                const [, , gid, ownerId] = id.split(':');

                if (interaction.guildId !== gid) {
                    return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
                }

                const wait = awaitingPhoto.get(k);
                awaitingPhoto.delete(k);

                if (!wait) {
                    await interaction.update({
                        content: '',
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });
                    uiMessages.set(k, new Set([interaction.message.id]));
                    return;
                }

                // 詳細画面から来た場合は詳細へ戻す
                if (wait.backTo === 'detail' && wait.postId) {
                    await ensureCacheLoadedForGuild(interaction.guild);
                    const cache = getGuildCache(guildId);
                    const post = cache.get(wait.postId);

                    if (post) {
                        const nav = getDetailNavState(guildId, userId, wait.postId);
                        const fromMine = nav?.fromMine ?? cameFromMine(k, wait.postId, mineState);
                        const forceHomeBack = nav?.forceHomeBack ?? false;

                        const { detail, components } = renderDetail(interaction, {
                            post,
                            guildId,
                            userId,
                            fromMine,
                            total: forceHomeBack ? 1 : (fromMine ? 1 : (searchState.get(k)?.results?.length || 1)),
                            forceHomeBack,
                        });

                        await interaction.update({
                            content: '',
                            embeds: [detail],
                            components,
                        });

                        uiMessages.set(k, new Set([interaction.message.id]));
                        return;
                    }

                    await interaction.update({
                        content: '',
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });

                    uiMessages.set(k, new Set([interaction.message.id]));
                    return;
                }

                // 新規登録後なら詳細を開くか確認
                if (wait.backTo === 'home' && wait.postId) {
                    await ensureCacheLoadedForGuild(interaction.guild);
                    const cache = getGuildCache(guildId);
                    const post = cache.get(wait.postId);

                    if (post) {
                        await interaction.update({
                            content: '',
                            embeds: [
                                confirmEmbed(
                                    '📄 詳細を開きますか？',
                                    `**${post.name}**`
                                )
                            ],
                            components: confirmComponents(
                                'openDetailAfterCreate',
                                guildId,
                                userId,
                                post.id
                            ),
                        });

                        uiMessages.set(k, new Set([interaction.message.id]));
                        return;
                    }

                    await interaction.update({
                        content: '',
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });

                    uiMessages.set(k, new Set([interaction.message.id]));
                    return;
                }

                await interaction.update({
                    content: '',
                    embeds: [homeEmbed()],
                    components: homeComponents(),
                });

                uiMessages.set(k, new Set([interaction.message.id]));
                return;
            }

            if (id.startsWith('delimg:')) {
                const [, dir, gid, ownerId, postId, idxStr, kind = 'deletePost'] = id.split(':');

                if (interaction.guildId !== gid) {
                    return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
                }

                await ensureCacheLoadedForGuild(interaction.guild);
                const cache = getGuildCache(guildId);
                const post = cache.get(postId);

                if (!post) {
                    return interaction.update({
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });
                }

                if (!canEdit(interaction, post)) {
                    return interaction.reply({
                        ephemeral: true,
                        content: '削除できるのは登録者または管理者のみです'
                    });
                }

                const urls = imageUrls(post);
                if (!urls.length) {
                    if (kind === 'deleteAllPhotos') {
                        return interaction.update({
                            content: '',
                            embeds: [confirmDeleteAllPhotosEmbed(post)],
                            components: confirmComponents('deleteAllPhotos', guildId, userId, postId, '0', false),
                        });
                    }

                    return interaction.update({
                        content: '',
                        embeds: [confirmDeletePostEmbed(post, { imageIndex: 0 })],
                        components: confirmComponents('deletePost', guildId, userId, postId, '0', false),
                    });
                }

                let idx = Math.max(0, Math.min(urls.length - 1, Number(idxStr) || 0));

                if (dir === 'prev') {
                    idx = (idx - 1 + urls.length) % urls.length;
                } else {
                    idx = (idx + 1) % urls.length;
                }

                if (kind === 'deleteAllPhotos') {
                    const embed = new EmbedBuilder()
                        .setTitle('⚠ 写真をすべて削除')
                        .setDescription(`**${post.name}** の写真をすべて削除しますか？\n写真 ${idx + 1}/${urls.length}`)
                        .setImage(urls[idx]);

                    return interaction.update({
                        content: '',
                        embeds: [embed],
                        components: confirmComponents('deleteAllPhotos', guildId, userId, postId, String(idx), urls.length > 1),
                    });
                }

                return interaction.update({
                    content: '',
                    embeds: [confirmDeletePostEmbed(post, { imageIndex: idx })],
                    components: confirmComponents('deletePost', guildId, userId, postId, String(idx), urls.length > 1),
                });
            }
        }

        // StringSelectMenu
        if (interaction.isStringSelectMenu()) {
            if (id.startsWith('create:tagPick:') || id.startsWith('edit:tagPick:')) {
                const parts = id.split(':');
                const mode = parts[0];
                const gid = parts[2];
                const ownerId = parts[3];

                if (interaction.guildId !== gid) {
                    return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
                }

                const d = draftRating.get(k);
                if (!d || d.mode !== mode) {
                    return interaction.reply({
                        ephemeral: true,
                        content: mode === 'create' ? '新規登録状態がありません' : '編集状態がありません'
                    });
                }

                d.tags = interaction.values ?? [];
                draftRating.set(k, d);

                return mode === 'create'
                    ? renderCreatePanel(interaction, guildId, userId, { update: true })
                    : renderEditPanel(interaction, guildId, userId, { update: true });
            }
        }

        if (id.startsWith('confirm:')) {
            const [, answer, kind, gid, ownerId, postId, extra] = id.split(':');

            if (interaction.guildId !== gid) {
                return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
            }

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);
            const post = cache.get(postId);

            if (kind === 'openDetailAfterCreate') {
                if (!post) {
                    await interaction.update({
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });
                    uiMessages.set(k, new Set([interaction.message.id]));
                    return;
                }

                if (answer === 'yes') {
                    const { detail, components } = renderDetail(interaction, {
                        post,
                        guildId,
                        userId,
                        fromMine: false,
                        total: 1,
                        forceHomeBack: true,
                    });

                    await interaction.update({
                        content: '',
                        embeds: [detail],
                        components,
                    });
                    uiMessages.set(k, new Set([interaction.message.id]));
                    return;
                }

                await interaction.update({
                    content: '',
                    embeds: [homeEmbed()],
                    components: homeComponents(),
                });
                uiMessages.set(k, new Set([interaction.message.id]));
                return;
            }

            if (answer === 'no') {
                // 写真1枚削除キャンセル → 写真管理へ戻す
                if (kind === 'deletePhoto') {
                    if (!post) {
                        return interaction.update({
                            content: '',
                            embeds: [homeEmbed()],
                            components: homeComponents(),
                        });
                    }

                    const urls = imageUrls(post);
                    const idx = Math.max(0, Math.min(urls.length - 1, Number(extra) || 0));

                    photoView.set(k, { postId, idx });

                    const embed = new EmbedBuilder()
                        .setTitle(`🖼 写真管理: ${post.name}`)
                        .setDescription(urls.length ? `写真 ${idx + 1}/${urls.length}` : '写真はありません')
                        .setImage(urls.length ? urls[idx] : null);

                    return interaction.update({
                        content: '',
                        embeds: [embed],
                        components: photoManagerComponents(guildId, ownerId, postId, urls.length),
                    });
                }

                // 全写真削除キャンセル → 写真管理へ戻す
                if (kind === 'deleteAllPhotos') {
                    if (!post) {
                        return interaction.update({
                            content: '',
                            embeds: [homeEmbed()],
                            components: homeComponents(),
                        });
                    }

                    const urls = imageUrls(post);
                    const pv = photoView.get(k) ?? { postId, idx: Math.max(0, urls.length - 1) };
                    if (pv.idx >= urls.length) pv.idx = Math.max(0, urls.length - 1);
                    photoView.set(k, pv);

                    const embed = new EmbedBuilder()
                        .setTitle(`🖼 写真管理: ${post.name}`)
                        .setDescription(urls.length ? `写真 ${pv.idx + 1}/${urls.length}` : '写真はありません')
                        .setImage(urls.length ? urls[pv.idx] : null);

                    return interaction.update({
                        content: '',
                        embeds: [embed],
                        components: photoManagerComponents(guildId, ownerId, postId, urls.length),
                    });
                }

                if (kind === 'deletePost') {
                    if (!post) {
                        return interaction.update({
                            content: '',
                            embeds: [homeEmbed()],
                            components: homeComponents(),
                        });
                    }

                    const nav = getDetailNavState(guildId, userId, postId);
                    const fromMine = nav?.fromMine ?? cameFromMine(k, postId, mineState);
                    const forceHomeBack = nav?.forceHomeBack ?? false;

                    const { detail, components } = renderDetail(interaction, {
                        post,
                        guildId,
                        userId,
                        fromMine,
                        total: forceHomeBack ? 1 : (fromMine ? 1 : (searchState.get(k)?.results?.length || 1)),
                        forceHomeBack,
                    });

                    return interaction.update({
                        content: '',
                        embeds: [detail],
                        components,
                    });
                }

                // 店削除や作成後確認など、他の confirm は閉じるだけ
                return interaction.update({
                    content: '',
                    embeds: [homeEmbed()],
                    components: homeComponents(),
                });
            }

            // ===== YES =====

            if (!post && kind !== 'deletePost') {
                return interaction.reply({ ephemeral: true, content: 'データが見つかりません' });
            }

            // 店情報をすべて削除
            if (kind === 'deletePost') {
                if (!post) {
                    return interaction.update({
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });
                }

                if (!canEdit(interaction, post)) {
                    return interaction.reply({ ephemeral: true, content: '削除できるのは登録者または管理者のみです' });
                }

                const fromMine = cameFromMine(k, postId, mineState);
                const dbCh = await getDbChannelForGuild(interaction.guild);

                for (const img of (post.images ?? [])) {
                    const msgId = typeof img === 'string' ? null : img?.msgId;
                    if (!msgId) continue;
                    try {
                        const m = await dbCh.messages.fetch(msgId);
                        await m.delete();
                    } catch { }
                }

                try {
                    const main = await dbCh.messages.fetch(postId);
                    await main.delete();
                } catch { }

                cache.delete(postId);

                const mine = mineState.get(k);
                if (mine?.results) {
                    mine.results = mine.results.filter(x => x !== postId);
                    mineState.set(k, mine);
                }

                const srch = searchState.get(k);
                if (srch?.results) {
                    srch.results = srch.results.filter(x => x !== postId);
                    srch.page = 0;
                    searchState.set(k, srch);
                }

                if (fromMine) {
                    return renderMineList(interaction, guildId, userId, { update: true });
                }

                const st = searchState.get(k);
                if (st?.results?.length) {
                    if (!st.results.length) {
                        return interaction.update({
                            embeds: [searchPanelEmbed(st)],
                            components: searchPanelComponents(guildId, userId, st),
                        });
                    }
                    return renderSearchResultList(interaction, guildId, userId, { update: true });
                }

                return interaction.update({
                    embeds: [homeEmbed()],
                    components: homeComponents(),
                });
            }

            // この写真削除
            if (kind === 'deletePhoto') {
                if (!post) return interaction.reply({ ephemeral: true, content: 'データが見つかりません' });
                if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '写真の編集は投稿者のみです' });

                const imgs = post.images ?? [];
                const idx = Math.max(0, Math.min(imgs.length - 1, Number(extra) || 0));
                const target = imgs[idx];
                const msgId = (typeof target === 'string') ? null : target?.msgId;

                if (!msgId) {
                    return interaction.reply({ ephemeral: true, content: '古い形式の写真のため、DBから削除できません（再登録が必要）' });
                }

                const dbCh = await getDbChannelForGuild(interaction.guild);
                try {
                    const imgMsg = await dbCh.messages.fetch(msgId);
                    await imgMsg.delete();
                } catch {
                    return interaction.reply({ ephemeral: true, content: 'DB側の写真メッセージ削除に失敗しました（権限/対象なし）' });
                }

                post.images.splice(idx, 1);
                post.updated_at = nowIso();

                const dbMsg = await dbCh.messages.fetch(postId);
                await dbMsg.edit({ embeds: [buildPostEmbedForDb(post)] });
                cache.set(postId, post);

                const urls = imageUrls(post);
                const newIdx = Math.max(0, Math.min(idx, urls.length - 1));
                photoView.set(k, { postId, idx: newIdx });

                const embed = new EmbedBuilder()
                    .setTitle(`🖼 写真管理: ${post.name}`)
                    .setDescription(urls.length ? `写真 ${newIdx + 1}/${urls.length}` : '写真はありません')
                    .setImage(urls.length ? urls[newIdx] : null);

                return interaction.update({
                    embeds: [embed],
                    components: photoManagerComponents(guildId, ownerId, postId, urls.length),
                });
            }

            // 写真すべて削除
            if (kind === 'deleteAllPhotos') {
                if (!post) return interaction.reply({ ephemeral: true, content: 'データが見つかりません' });
                if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '写真の編集は投稿者のみです' });

                const dbCh = await getDbChannelForGuild(interaction.guild);

                for (const img of (post.images ?? [])) {
                    const msgId = (typeof img === 'string') ? null : img?.msgId;
                    if (!msgId) continue;
                    try {
                        const imgMsg = await dbCh.messages.fetch(msgId);
                        await imgMsg.delete();
                    } catch (e) {
                        console.error('delete all photo failed:', e);
                    }
                }

                post.images = [];
                post.updated_at = nowIso();

                const dbMsg = await dbCh.messages.fetch(postId);
                await dbMsg.edit({ embeds: [buildPostEmbedForDb(post)] });
                cache.set(postId, post);

                photoView.set(k, { postId, idx: 0 });

                const embed = new EmbedBuilder()
                    .setTitle(`🖼 写真管理: ${post.name}`)
                    .setDescription('写真はありません');

                return interaction.update({
                    embeds: [embed],
                    components: photoManagerComponents(guildId, ownerId, postId, false),
                });
            }
        }

        if (id.startsWith('date:input:')) {
            const [, action, gid, ownerId, mode, postId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== mode) {
                return interaction.reply({ ephemeral: true, content: '途中状態がありません最初からやり直してください' });
            }

            const currentValue = d.visitedDate ?? '';

            return interaction.showModal(
                buildVisitedDateModal(gid, ownerId, mode, postId || '', currentValue)
            );
        }

        if (id.startsWith('search:prefPagePrev:') || id.startsWith('search:prefPageNext:')) {
            const parts = id.split(':');
            const gid = parts[2];
            const ownerId = parts[3];
            const page = Number(parts[4] || 0);

            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const next = id.includes('prefPageNext') ? page + 1 : page - 1;
            const { p, totalPages, slice } = prefSlice(next);

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            const select = new StringSelectMenuBuilder()
                .setCustomId(`search:prefPick:${guildId}:${ownerId}:${p}`)
                .setPlaceholder(`都道府県を選択してください（複数可） ${p + 1}/${totalPages}`)
                .setMinValues(0)
                .setMaxValues(slice.length)
                .addOptions(
                    slice.map(x => ({
                        label: x,
                        value: x,
                        default: st.prefectureFilters?.includes(x) ?? false,
                    }))
                );

            const nav = new ActionRowBuilder().addComponents(
                new ButtonBuilder()
                    .setCustomId(`search:prefPagePrev:${guildId}:${ownerId}:${p}`)
                    .setLabel('◀ 前へ')
                    .setStyle(ButtonStyle.Secondary)
                    .setDisabled(p <= 0),

                new ButtonBuilder()
                    .setCustomId(`search:prefPageNext:${guildId}:${ownerId}:${p}`)
                    .setLabel('次へ ▶')
                    .setStyle(ButtonStyle.Secondary)
                    .setDisabled(p >= totalPages - 1),

                new ButtonBuilder()
                    .setCustomId(`search:prefPageClear:${guildId}:${ownerId}`)
                    .setLabel('解除')
                    .setStyle(ButtonStyle.Secondary),

                new ButtonBuilder()
                    .setCustomId(`search:prefBack:${guildId}:${ownerId}`)
                    .setLabel('戻る')
                    .setStyle(ButtonStyle.Secondary),
            );

            return interaction.update({
                content: '都道府県を選択してください',
                components: [new ActionRowBuilder().addComponents(select), nav],
            });
        }

        if (id.startsWith('search:prefPageClear:')) {
            const parts = id.split(':');
            const gid = parts[2];
            const ownerId = parts[3];

            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };
            st.prefectureFilters = [];
            searchState.set(k, st);

            return interaction.update({
                content: '都道府県フィルタを解除しました',
                embeds: [searchPanelEmbed(st)],
                components: searchPanelComponents(guildId, userId, st),
            });
        }

        // 都道府県ピッカー ページ送り（create/edit共通）
        if (id.startsWith('create:prefPagePrev:') || id.startsWith('create:prefPageNext:') ||
            id.startsWith('edit:prefPagePrev:') || id.startsWith('edit:prefPageNext:')) {

            const parts = id.split(':');
            const mode = parts[0];
            const gid = parts[2];
            const ownerId = parts[3];
            const page = Number(parts[4] || 0);

            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const next = id.includes('prefPageNext') ? page + 1 : page - 1;

            // 同じephemeralメッセージを更新する（増やさない）
            await interaction.update({
                content: '都道府県を選択してください（任意）',
                components: [
                    ...prefPickComponents(mode, gid, ownerId, next),
                    new ActionRowBuilder().addComponents(
                        new ButtonBuilder()
                            .setCustomId(`${mode}:panelBack:${gid}:${ownerId}`)
                            .setLabel('戻る')
                            .setStyle(ButtonStyle.Secondary)
                    ),
                ],
            });

            return;
        }

        if (id.startsWith('create:setName:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) {
                return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
            }

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildNameModal(gid, ownerId, d.name ?? ''));
        }

        // Home
        if (id.startsWith('create:setVisit:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('✅ 訪問状態').setDescription('訪問状態を選択してください')],
                components: [
                    new ActionRowBuilder().addComponents(
                        new ButtonBuilder().setCustomId(`create:visit:visited:${gid}:${ownerId}`).setLabel('✅ 行った').setStyle(ButtonStyle.Secondary),
                        new ButtonBuilder().setCustomId(`create:visit:planned:${gid}:${ownerId}`).setLabel('📝 行きたい').setStyle(ButtonStyle.Secondary),
                        new ButtonBuilder().setCustomId(`create:panelBack:${gid}:${ownerId}`).setLabel('戻る').setStyle(ButtonStyle.Secondary),
                    )
                ],
            });
        }

        if (id.startsWith('create:visit:')) {
            const [, , kind, gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== 'create') {
                return interaction.reply({ ephemeral: true, content: '新規登録状態がありません' });
            }

            d.visited = kind === 'visited';

            draftRating.set(k, d);
            return renderCreatePanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('create:setRating:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('⭐ 評価').setDescription('評価を選択してください')],
                components: [
                    new ActionRowBuilder().addComponents(
                        new ButtonBuilder().setCustomId(`create:rating:1:${gid}:${ownerId}`).setLabel('⭐').setStyle(ButtonStyle.Primary),
                        new ButtonBuilder().setCustomId(`create:rating:2:${gid}:${ownerId}`).setLabel('⭐⭐').setStyle(ButtonStyle.Primary),
                        new ButtonBuilder().setCustomId(`create:rating:3:${gid}:${ownerId}`).setLabel('⭐⭐⭐').setStyle(ButtonStyle.Primary),
                        new ButtonBuilder().setCustomId(`create:rating:4:${gid}:${ownerId}`).setLabel('⭐⭐⭐⭐').setStyle(ButtonStyle.Primary),
                        new ButtonBuilder().setCustomId(`create:rating:5:${gid}:${ownerId}`).setLabel('⭐⭐⭐⭐⭐').setStyle(ButtonStyle.Primary),
                    ),
                    new ActionRowBuilder().addComponents(
                        new ButtonBuilder().setCustomId(`create:panelBack:${gid}:${ownerId}`).setLabel('戻る').setStyle(ButtonStyle.Secondary),
                    )
                ],
            });
        }

        if (id.startsWith('create:rating:')) {
            const [, , ratingStr, gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== 'create') {
                return interaction.reply({ ephemeral: true, content: '新規登録状態がありません' });
            }

            d.rating = Number(ratingStr);
            draftRating.set(k, d);

            return renderCreatePanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('create:setComment:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildCommentModal(gid, ownerId, d.comment ?? ''));
        }

        if (id.startsWith('create:setTags:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('🏷 タグ').setDescription('入力方法を選択してください')],
                components: tagEntryChoiceComponents('create', gid, ownerId),
            });
        }

        if (id.startsWith('create:tagsExisting:') || id.startsWith('edit:tagsExisting:')) {
            const parts = id.split(':');
            const mode = parts[0];
            const gid = parts[2];
            const ownerId = parts[3];

            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);

            const d = draftRating.get(k);
            if (!d || d.mode !== mode) {
                return interaction.reply({
                    ephemeral: true,
                    content: mode === 'create' ? '新規登録状態がありません' : '編集状態がありません'
                });
            }

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('🏷 既存タグ選択').setDescription('タグを選択してください')],
                components: tagPickComponents(mode, gid, ownerId, cache, d.tags ?? [], 0),
            });
        }

        if (id.startsWith('create:tagsNew:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildTagsModal(gid, ownerId, (d.tags ?? []).join(', ')));
        }

        if (id.startsWith('edit:tagsNew:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildEditTagsModal(gid, ownerId, (d.tags ?? []).join(', ')));
        }

        if (
            id.startsWith('create:tagPagePrev:') || id.startsWith('create:tagPageNext:') ||
            id.startsWith('edit:tagPagePrev:') || id.startsWith('edit:tagPageNext:')
        ) {
            const parts = id.split(':');
            const mode = parts[0];
            const gid = parts[2];
            const ownerId = parts[3];
            const page = Number(parts[4] || 0);

            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);

            const d = draftRating.get(k);
            if (!d || d.mode !== mode) {
                return interaction.reply({
                    ephemeral: true,
                    content: mode === 'create' ? '新規登録状態がありません' : '編集状態がありません'
                });
            }

            const next = id.includes('tagPageNext') ? page + 1 : page - 1;

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('🏷 既存タグ選択').setDescription('タグを選択してください')],
                components: tagPickComponents(mode, gid, ownerId, cache, d.tags ?? [], next),
            });
        }

        if (id.startsWith('create:tagClear:') || id.startsWith('edit:tagClear:')) {
            const parts = id.split(':');
            const mode = parts[0];
            const gid = parts[2];
            const ownerId = parts[3];

            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== mode) {
                return interaction.reply({
                    ephemeral: true,
                    content: mode === 'create' ? '新規登録状態がありません' : '編集状態がありません'
                });
            }

            d.tags = [];
            draftRating.set(k, d);

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('🏷 タグ').setDescription('入力方法を選択してください')],
                components: tagEntryChoiceComponents(mode, gid, ownerId),
            });
        }

        if (id.startsWith('create:tagChoiceBack:') || id.startsWith('edit:tagChoiceBack:')) {
            const parts = id.split(':');
            const mode = parts[0];
            const gid = parts[2];
            const ownerId = parts[3];

            if (interaction.guildId !== gid) {
                return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
            }

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('🏷 タグ').setDescription('入力方法を選択してください')],
                components: tagEntryChoiceComponents(mode, gid, ownerId),
            });
        }

        if (id.startsWith('create:setUrl:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildUrlModal(gid, ownerId, d.url ?? ''));
        }

        if (id.startsWith('create:setMap:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildMapModal(gid, ownerId, d.mapUrl ?? ''));
        }

        if (id.startsWith('create:setDate:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildVisitedDateModal(gid, ownerId, 'create', '', d.visitedDate ?? ''));
        }

        if (id.startsWith('create:setPref:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('🗾 都道府県').setDescription('都道府県を選択してください')],
                components: [
                    ...prefPickComponents('create', gid, ownerId, 0),
                    new ActionRowBuilder().addComponents(
                        new ButtonBuilder().setCustomId(`create:panelBack:${gid}:${ownerId}`).setLabel('戻る').setStyle(ButtonStyle.Secondary)
                    )
                ],
            });
        }

        if (id.startsWith('create:panelBack:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            return renderCreatePanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('create:reset:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            draftRating.set(k, {
                mode: 'create',
                postId: null,
                visited: null,
                rating: null,
                comment: '',
                prefecture: '',
                visitedDate: '',
                tags: [],
                url: '',
                mapUrl: '',
                name: '',
                channelId: interaction.channelId,
            });

            return renderCreatePanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('create:cancel:') || id.startsWith('create:home:')) {
            const [, action, gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            draftRating.delete(k);

            return interaction.update({
                content: '',
                embeds: [homeEmbed()],
                components: homeComponents(),
            });
        }

        if (id.startsWith('create:photos:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            return interaction.reply({
                ephemeral: true,
                content: '写真は登録完了後に追加してください',
            });
        }

        if (id.startsWith('create:submit:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) {
                return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
            }

            const d = draftRating.get(k);
            if (!d || d.mode !== 'create') {
                return interaction.reply({ ephemeral: true, content: '新規登録状態がありません' });
            }

            // 店名必須
            if (!d.name || !d.name.trim()) {
                return interaction.reply({
                    ephemeral: true,
                    content: 'お店の名前を入力してください'
                });
            }

            // 訪問状態必須
            if (d.visited == null) {
                return interaction.reply({
                    ephemeral: true,
                    content: '訪問状態を選択してください'
                });
            }

            // 「行った」のときは評価必須
            if (d.visited === true && (d.rating == null || ![1, 2, 3, 4, 5].includes(Number(d.rating)))) {
                return interaction.reply({
                    ephemeral: true,
                    content: '「行った」を選んだ場合は評価を選択してください'
                });
            }

            await interaction.deferUpdate();

            const dbCh = await getDbChannelForGuild(interaction.guild);
            const cache = getGuildCache(guildId);

            const post = {
                id: '',
                name: d.name.trim(),
                visited: d.visited !== false,
                rating: d.visited === false ? null : Number(d.rating),
                comment: d.comment ?? '',
                url: d.url ?? '',
                map_url: d.mapUrl ?? '',
                visited_date: d.visited === false ? '' : (d.visitedDate ?? ''),
                tags: d.tags ?? [],
                prefecture: d.prefecture ?? '',
                images: [],
                created_by: userId,
                created_at: nowIso(),
                updated_at: nowIso(),
            };

            // いったん仮IDでembed生成して送信
            const sent = await dbCh.send({
                embeds: [buildPostEmbedForDb({ ...post, id: 'TEMP' })],
            });

            // 本当のIDを設定
            post.id = sent.id;

            // 正しいIDでembedを更新
            await sent.edit({
                embeds: [buildPostEmbedForDb(post)],
            });

            // cacheへ保存
            cache.set(post.id, post);

            // 自分の記録一覧にも反映
            const mine = mineState.get(k);
            if (mine?.results) {
                mine.results.unshift(post.id);
                mineState.set(k, mine);
            }

            // 作成ドラフトは消す
            draftRating.delete(k);
            createPanelPromptRef.delete(k);

            // 先に待機画面へ更新
            const waitingMsg = await interaction.editReply({
                content: '',
                embeds: [photoCreateWaitingEmbed(post.name)],
                components: photoWaitingComponentsForCreate(guildId, userId),
            });

            // 写真待ち状態へ
            awaitingPhoto.set(k, {
                postId: post.id,
                channelId: interaction.channelId,
                guildId,
                backTo: 'home',
                uiMessageRef: {
                    webhook: interaction.webhook,
                    messageId: waitingMsg?.id,
                },
            });

            return;
        }

        if (id.startsWith('edit:setVisit:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('✅ 訪問状態').setDescription('訪問状態を選択してください')],
                components: [
                    new ActionRowBuilder().addComponents(
                        new ButtonBuilder().setCustomId(`edit:visit:visited:${gid}:${ownerId}`).setLabel('✅ 行った').setStyle(ButtonStyle.Secondary),
                        new ButtonBuilder().setCustomId(`edit:visit:planned:${gid}:${ownerId}`).setLabel('📝 行きたい').setStyle(ButtonStyle.Secondary),
                        new ButtonBuilder().setCustomId(`edit:panelBack:${gid}:${ownerId}`).setLabel('戻る').setStyle(ButtonStyle.Secondary),
                    )
                ],
            });
        }

        if (id.startsWith('edit:setName:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) {
                return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
            }

            const d = draftRating.get(k);
            if (!d || d.mode !== 'edit') {
                return interaction.reply({ ephemeral: true, content: '編集状態がありません' });
            }

            return interaction.showModal(buildEditNameModal(gid, ownerId, d.name ?? ''));
        }

        if (id.startsWith('edit:visit:')) {
            const [, , kind, gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== 'edit') {
                return interaction.reply({ ephemeral: true, content: '編集状態がありません' });
            }

            d.visited = kind === 'visited';

            draftRating.set(k, d);
            return renderEditPanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('edit:setRating:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('⭐ 評価').setDescription('評価を選択してください')],
                components: [
                    new ActionRowBuilder().addComponents(
                        new ButtonBuilder().setCustomId(`edit:rating:1:${gid}:${ownerId}`).setLabel('⭐').setStyle(ButtonStyle.Primary),
                        new ButtonBuilder().setCustomId(`edit:rating:2:${gid}:${ownerId}`).setLabel('⭐⭐').setStyle(ButtonStyle.Primary),
                        new ButtonBuilder().setCustomId(`edit:rating:3:${gid}:${ownerId}`).setLabel('⭐⭐⭐').setStyle(ButtonStyle.Primary),
                        new ButtonBuilder().setCustomId(`edit:rating:4:${gid}:${ownerId}`).setLabel('⭐⭐⭐⭐').setStyle(ButtonStyle.Primary),
                        new ButtonBuilder().setCustomId(`edit:rating:5:${gid}:${ownerId}`).setLabel('⭐⭐⭐⭐⭐').setStyle(ButtonStyle.Primary),
                    ),
                    new ActionRowBuilder().addComponents(
                        new ButtonBuilder().setCustomId(`edit:panelBack:${gid}:${ownerId}`).setLabel('戻る').setStyle(ButtonStyle.Secondary),
                    )
                ],
            });
        }

        if (id.startsWith('edit:rating:')) {
            const [, , ratingStr, gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== 'edit') {
                return interaction.reply({ ephemeral: true, content: '編集状態がありません' });
            }

            d.rating = Number(ratingStr);
            draftRating.set(k, d);

            return renderEditPanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('edit:setComment:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildEditCommentModal(gid, ownerId, d.comment ?? ''));
        }

        if (id.startsWith('edit:setTags:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('🏷 タグ').setDescription('入力方法を選択してください')],
                components: tagEntryChoiceComponents('edit', gid, ownerId),
            });
        }

        if (id.startsWith('edit:setUrl:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildEditUrlModal(gid, ownerId, d.url ?? ''));
        }

        if (id.startsWith('edit:setMap:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildEditMapModal(gid, ownerId, d.mapUrl ?? ''));
        }

        if (id.startsWith('edit:setDate:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildVisitedDateModal(gid, ownerId, 'edit', '', d.visitedDate ?? ''));
        }

        if (id.startsWith('edit:setPref:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('🗾 都道府県').setDescription('都道府県を選択してください')],
                components: [
                    ...prefPickComponents('edit', gid, ownerId, 0),
                    new ActionRowBuilder().addComponents(
                        new ButtonBuilder().setCustomId(`edit:panelBack:${gid}:${ownerId}`).setLabel('戻る').setStyle(ButtonStyle.Secondary)
                    )
                ],
            });
        }

        if (id.startsWith('edit:panelBack:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            return renderEditPanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('edit:back:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) {
                return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
            }

            const d = draftRating.get(k);
            if (!d || d.mode !== 'edit' || !d.postId) {
                return interaction.reply({ ephemeral: true, content: '編集状態がありません' });
            }

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);
            const post = cache.get(d.postId);

            if (!post) {
                return interaction.update({
                    content: '',
                    embeds: [homeEmbed()],
                    components: homeComponents(),
                });
            }

            const nav = getDetailNavState(guildId, userId, post.id);
            const fromMine = nav?.fromMine ?? cameFromMine(k, post.id, mineState);
            const forceHomeBack = nav?.forceHomeBack ?? false;

            const { detail, components } = renderDetail(interaction, {
                post,
                guildId,
                userId,
                fromMine,
                total: forceHomeBack ? 1 : (fromMine ? 1 : (searchState.get(k)?.results?.length || 1)),
                forceHomeBack,
            });

            return interaction.update({
                content: '',
                embeds: [detail],
                components,
            });
        }

        if (id.startsWith('edit:photos:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== 'edit' || !d.postId) {
                return interaction.reply({ ephemeral: true, content: '編集状態がありません' });
            }

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);
            const post = cache.get(d.postId);
            if (!post) return interaction.reply({ ephemeral: true, content: 'データが見つかりません' });

            const urls = imageUrls(post);
            photoView.set(k, { postId: post.id, idx: Math.max(0, urls.length - 1) });

            const embed = new EmbedBuilder()
                .setTitle(`🖼 写真管理: ${post.name}`)
                .setDescription(urls.length ? `写真 ${Math.max(0, urls.length - 1) + 1}/${urls.length}` : '写真はありません')
                .setImage(urls.length ? urls[Math.max(0, urls.length - 1)] : null);

            return interaction.update({
                embeds: [embed],
                components: photoManagerComponents(guildId, ownerId, post.id, urls.length),
            });
        }

        if (id.startsWith('edit:submit:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== 'edit' || !d.postId) {
                return interaction.reply({ ephemeral: true, content: '編集状態がありません' });
            }

            if (!d.name?.trim()) {
                return interaction.reply({ ephemeral: true, content: 'お店の名前は必須です' });
            }

            if (d.visited === true && (d.rating == null || ![1, 2, 3, 4, 5].includes(Number(d.rating)))) {
                return interaction.reply({
                    ephemeral: true,
                    content: '「行った」を選んだ場合は評価を選択してください'
                });
            }

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);
            const post = cache.get(d.postId);
            if (!post) return interaction.reply({ ephemeral: true, content: '対象データが見つかりません' });
            if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '編集できません（投稿者のみ）' });

            post.name = d.name.trim();
            post.comment = d.comment ?? '';
            post.url = d.url ?? '';
            post.map_url = d.mapUrl ?? '';
            post.tags = d.tags ?? [];
            post.visited = d.visited !== false;
            post.rating = d.visited === false ? null : d.rating;
            post.visited_date = d.visited === false ? '' : (d.visitedDate ?? '');
            post.prefecture = d.prefecture ?? '';
            post.updated_at = nowIso();

            const dbCh = await getDbChannelForGuild(interaction.guild);
            const dbMsg = await dbCh.messages.fetch(post.id);
            await dbMsg.edit({ embeds: [buildPostEmbedForDb(post)] });

            cache.set(post.id, post);

            draftRating.delete(k);
            editPanelPromptRef.delete(k);

            const nav = getDetailNavState(guildId, userId, post.id);
            const fromMine = nav?.fromMine ?? cameFromMine(k, post.id, mineState);
            const forceHomeBack = nav?.forceHomeBack ?? false;

            const { detail, components } = renderDetail(interaction, {
                post,
                guildId,
                userId,
                fromMine,
                total: forceHomeBack ? 1 : (fromMine ? 1 : (searchState.get(k)?.results?.length || 1)),
                forceHomeBack,
            });

            await interaction.update({
                content: '',
                embeds: [detail],
                components,
            });

            const okMsg = await interaction.followUp({
                content: '✅ 更新しました',
                ephemeral: true,
                fetchReply: true,
            });

            setTimeout(() => {
                okMsg.delete().catch(() => { });
            }, 2000);

            return;
        }

        if (id === 'home:create') {
            draftRating.set(k, {
                mode: 'create',
                postId: null,
                visited: null,
                rating: null,
                comment: '',
                prefecture: '',
                visitedDate: '',
                tags: [],
                url: '',
                mapUrl: '',
                name: '',
                channelId: interaction.channelId,
            });

            await renderCreatePanel(interaction, guildId, userId, { update: true });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }
        if (id === 'home:search') {
            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            searchState.set(k, st);

            await interaction.update({
                content: '',
                embeds: [searchPanelEmbed(st)],
                components: searchPanelComponents(guildId, userId, st),
            });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        if (id === 'home:mine') {
            await interaction.deferUpdate();

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);

            const mine = [...cache.values()]
                .filter(p => p.created_by === userId)
                .sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''));

            mineState.set(k, {
                results: mine.map(p => p.id),
                page: 0,
                visitFilter: 'all',
            });

            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return renderMineList(interaction, guildId, userId, { update: true });
        }

        // Search panel
        if (id.startsWith('search:setUser:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            const menu = new UserSelectMenuBuilder()
                .setCustomId(`search:userPick:${guildId}:${ownerId}`)
                .setPlaceholder('登録者を選択してください（複数可）')
                .setMinValues(0)
                .setMaxValues(25);

            if (st.userIdFilter?.length) {
                menu.setDefaultUsers(...st.userIdFilter);
            }

            const row1 = new ActionRowBuilder().addComponents(menu);

            const row2 = new ActionRowBuilder().addComponents(
                new ButtonBuilder()
                    .setCustomId(`search:userClear:${guildId}:${ownerId}`)
                    .setLabel('解除')
                    .setStyle(ButtonStyle.Secondary),

                new ButtonBuilder()
                    .setCustomId(`search:userBack:${guildId}:${ownerId}`)
                    .setLabel('戻る')
                    .setStyle(ButtonStyle.Secondary)
            );

            await interaction.update({
                content: '検索する登録者を選択してください',
                embeds: [],
                components: [row1, row2],
            });

            return;
        }

        if (id.startsWith('search:userClear:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            st.userIdFilter = null;
            searchState.set(k, st);

            return interaction.update({
                content: '',
                embeds: [searchPanelEmbed(st)],
                components: searchPanelComponents(guildId, userId, st),
            });
        }

        if (id.startsWith('search:userBack:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            return interaction.update({
                content: '',
                embeds: [searchPanelEmbed(st)],
                components: searchPanelComponents(guildId, userId, st),
            });
        }

        if (id.startsWith('search:prefBack:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) {
                return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
            }

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                tagFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            return interaction.update({
                content: '',
                embeds: [searchPanelEmbed(st)],
                components: searchPanelComponents(guildId, userId, st),
            });
        }
        if (id.startsWith('search:setPref:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const { p, totalPages, slice } = prefSlice(0);

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            const select = new StringSelectMenuBuilder()
                .setCustomId(`search:prefPick:${guildId}:${ownerId}:${p}`)
                .setPlaceholder(`都道府県を選択してください（複数可） ${p + 1}/${totalPages}`)
                .setMinValues(0)
                .setMaxValues(slice.length)
                .addOptions(
                    slice.map(x => ({
                        label: x,
                        value: x,
                        default: st.prefectureFilters?.includes(x) ?? false,
                    }))
                );

            const nav = new ActionRowBuilder().addComponents(
                new ButtonBuilder()
                    .setCustomId(`search:prefPagePrev:${guildId}:${ownerId}:${p}`)
                    .setLabel('◀ 前へ')
                    .setStyle(ButtonStyle.Secondary)
                    .setDisabled(p <= 0),

                new ButtonBuilder()
                    .setCustomId(`search:prefPageNext:${guildId}:${ownerId}:${p}`)
                    .setLabel('次へ ▶')
                    .setStyle(ButtonStyle.Secondary)
                    .setDisabled(p >= totalPages - 1),

                new ButtonBuilder()
                    .setCustomId(`search:prefPageClear:${guildId}:${ownerId}`)
                    .setLabel('解除')
                    .setStyle(ButtonStyle.Secondary),

                new ButtonBuilder()
                    .setCustomId(`search:prefBack:${guildId}:${ownerId}`)
                    .setLabel('戻る')
                    .setStyle(ButtonStyle.Secondary),
            );

            await interaction.update({
                content: '都道府県を選択してください',
                embeds: [],
                components: [new ActionRowBuilder().addComponents(select), nav],
            });

            return;
        }

        if (id.startsWith('search:setText:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            // ここで元の検索パネル参照を保存
            searchKeywordPromptRef.set(k, {
                webhook: interaction.webhook,
                messageId: interaction.message?.id,
            });

            const modal = new ModalBuilder()
                .setCustomId(`modalSearch:${guildId}:${ownerId}`)
                .setTitle('🔎 検索条件');

            modal.addComponents(
                new ActionRowBuilder().addComponents(
                    new TextInputBuilder()
                        .setCustomId('keyword')
                        .setLabel('キーワード（スペース区切り）')
                        .setPlaceholder('例：ラーメン 東京 深夜')
                        .setStyle(TextInputStyle.Short)
                        .setRequired(false)
                        .setValue(st.keyword ?? '')
                )
            );

            return interaction.showModal(modal);
        }

        if (id.startsWith('search:clear:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const st = { userIdFilter: null, prefectureFilters: [], keyword: '', ratingFilters: [], results: [], page: 0 };
            searchState.set(k, st);
            return interaction.update({ embeds: [searchPanelEmbed(st)], components: searchPanelComponents(guildId, userId, st) });
        }

        if (id.startsWith('search:back:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await interaction.update({ embeds: [homeEmbed()], components: homeComponents() });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        if (id.startsWith('search:run:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            const keywords = (st.keyword ?? '')
                .normalize('NFKC')
                .toLowerCase()
                .trim()
                .split(/[ \u3000]+/)
                .filter(Boolean);

            const results = [...cache.values()]
                .filter(p => {
                    if (st.userIdFilter?.length) {
                        if (!st.userIdFilter.includes(p.created_by)) return false;
                    }

                    if (st.prefectureFilters?.length) {
                        const pp = (p.prefecture ?? '').trim();
                        if (!st.prefectureFilters.includes(pp)) return false;
                    }

                    if (keywords.length) {
                        const hay = [
                            p.name ?? '',
                            p.comment ?? '',
                            p.prefecture ?? '',
                            ...(p.tags ?? [])
                        ]
                            .join('\n')
                            .normalize('NFKC')
                            .toLowerCase();

                        for (const kw of keywords) {
                            if (!hay.includes(kw)) return false;
                        }
                    }

                    if (st.ratingFilters?.length) {
                        if (!st.ratingFilters.includes(Number(p.rating))) return false;
                    }

                    return true;
                })
                .sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''));

            st.results = results.map(p => p.id);
            st.page = 0;
            searchState.set(k, st);

            if (!st.results.length) {
                await interaction.update({
                    content: '該当する記録がありません',
                    embeds: [searchPanelEmbed(st)],
                    components: searchPanelComponents(guildId, userId, st),
                });
                await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
                return;
            }

            return renderSearchResultList(interaction, guildId, userId, { update: true });
        }

        // ⭐評価フィルター
        if (id.startsWith('search:setRating:')) {
            const [, , gid, ownerId, ratingStr] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const rating = Number(ratingStr);
            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            st.ratingFilters = Array.isArray(st.ratingFilters) ? st.ratingFilters : [];

            if (st.ratingFilters.includes(rating)) {
                st.ratingFilters = st.ratingFilters.filter(x => x !== rating);
            } else {
                st.ratingFilters.push(rating);
                st.ratingFilters.sort((a, b) => a - b);
            }

            searchState.set(k, st);

            return interaction.update({
                embeds: [searchPanelEmbed(st)],
                components: searchPanelComponents(guildId, userId, st),
            });
        }

        if (id.startsWith('res:prev:') || id.startsWith('res:next:')) {
            const [, action, gid, ownerId, postId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);
            const post = cache.get(postId);
            if (!post) return interaction.reply({ ephemeral: true, content: 'データが見つかりません' });

            const urls = imageUrls(post);
            if (urls.length <= 1) {
                const nav = getDetailNavState(guildId, userId, postId);
                const fromMine = nav?.fromMine ?? cameFromMine(k, postId, mineState);
                const forceHomeBack = nav?.forceHomeBack ?? false;

                const { detail, components } = renderDetail(interaction, {
                    post,
                    guildId,
                    userId,
                    fromMine,
                    total: forceHomeBack ? 1 : (fromMine ? 1 : (searchState.get(k)?.results?.length || 1)),
                    forceHomeBack,
                });

                return interaction.update({
                    content: '',
                    embeds: [detail],
                    components,
                });
            }

            const pv = detailPhotoView.get(k) ?? { postId, idx: Math.max(0, urls.length - 1) };
            pv.postId = postId;

            if (action === 'prev') {
                pv.idx = (pv.idx - 1 + urls.length) % urls.length;
            } else {
                pv.idx = (pv.idx + 1) % urls.length;
            }

            detailPhotoView.set(k, pv);

            const nav = getDetailNavState(guildId, userId, postId);
            const fromMine = nav?.fromMine ?? cameFromMine(k, postId, mineState);
            const forceHomeBack = nav?.forceHomeBack ?? false;

            const { detail, components } = renderDetail(interaction, {
                post,
                guildId,
                userId,
                fromMine,
                total: forceHomeBack ? 1 : (fromMine ? 1 : (searchState.get(k)?.results?.length || 1)),
                forceHomeBack,
            });

            return interaction.update({
                content: '',
                embeds: [detail],
                components,
            });
        }

        // Share
        if (id.startsWith('res:share:')) {
            const [, , gid, ownerId, postId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild);

            // ここから：DB正本を必ず読み直す
            const dbCh = await getDbChannelForGuild(interaction.guild);
            const dbMsg = await dbCh.messages.fetch(postId);
            const fresh = extractPostFromMessage(dbMsg);
            if (!fresh) return interaction.reply({ ephemeral: true, content: 'データが見つかりません' });

            // ここ追加：既存のimagesを保持
            const cache = getGuildCache(guildId);
            const existed = cache.get(postId);
            fresh.images = existed?.images ?? [];

            cache.set(postId, fresh);

            const chunks = buildDetailEmbedsChunks(fresh, { sharedByUserId: userId });
            for (const embeds of chunks) {
                await interaction.channel.send({ embeds });
            }

            const nav = getDetailNavState(guildId, userId, postId);
            const fromMine = nav?.fromMine ?? cameFromMine(k, postId, mineState);
            const forceHomeBack = nav?.forceHomeBack ?? false;

            const { detail, components } = renderDetail(interaction, {
                post: fresh,
                guildId,
                userId,
                fromMine,
                total: forceHomeBack ? 1 : (fromMine ? 1 : (searchState.get(k)?.results?.length || 1)),
                forceHomeBack,
                notice: '📤 このチャンネルに送信しました',
            });

            await interaction.update({
                content: '',
                embeds: [detail],
                components,
            });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        // Edit from result (only if canEdit)
        if (id.startsWith('res:edit:')) {
            const [, , gid, ownerId, postId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);
            const post = cache.get(postId);
            if (!post) return interaction.reply({ ephemeral: true, content: 'データが見つかりません' });
            if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '編集できません（投稿者のみ）' });

            const nav = getDetailNavState(guildId, userId, postId);
            setDetailNavState(guildId, userId, postId, {
                fromMine: nav?.fromMine ?? cameFromMine(k, postId, mineState),
                forceHomeBack: nav?.forceHomeBack ?? false,
            });

            draftRating.set(k, {
                mode: 'edit',
                postId,
                visited: post.visited !== false,
                rating: post.rating ?? null,
                comment: post.comment ?? '',
                prefecture: post.prefecture ?? '',
                visitedDate: post.visited_date ?? '',
                tags: post.tags ?? [],
                url: post.url ?? '',
                mapUrl: post.map_url ?? '',
                name: post.name ?? '',
                channelId: interaction.channelId,
            });

            await renderEditPanel(interaction, guildId, userId, { update: true });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        // Photos from result (only if canEdit)
        if (id.startsWith('res:photos:')) {
            const [, , gid, ownerId, postId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);
            const post = cache.get(postId);
            if (!post) return interaction.reply({ ephemeral: true, content: 'データが見つかりません' });
            if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '写真の編集は投稿者のみです' });

            const urls = imageUrls(post);

            photoView.set(k, { postId, idx: Math.max(0, urls.length - 1) });
            const pv = photoView.get(k);
            const total = urls.length;

            const embed = new EmbedBuilder()
                .setTitle(`🖼 写真管理: ${post.name}`)
                .setDescription(total ? `写真 ${pv.idx + 1}/${total}` : '写真はありません')
                .setImage(total ? urls[pv.idx] : null);

            return interaction.update({
                embeds: [embed],
                components: photoManagerComponents(guildId, ownerId, postId, total),
            });
        }

        if (id.startsWith('res:delete:')) {
            const [, , gid, ownerId, postId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);
            const post = cache.get(postId);
            if (!post) return interaction.reply({ ephemeral: true, content: 'データが見つかりません' });
            if (!canEdit(interaction, post)) {
                return interaction.reply({ ephemeral: true, content: '削除できるのは登録者または管理者のみです' });
            }

            const urls = imageUrls(post);
            const k = keyOf(guildId, userId);
            const pv = detailPhotoView.get(k);
            const idx = pv?.postId === postId
                ? Math.max(0, Math.min(urls.length - 1, Number(pv.idx) || 0))
                : Math.max(0, urls.length - 1);

            return interaction.update({
                content: '',
                embeds: [confirmDeletePostEmbed(post, { imageIndex: idx })],
                components: confirmComponents('deletePost', guildId, userId, postId, String(idx), urls.length > 1),
            });
        }

        if (id.startsWith('search:listBack:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            return renderSearchResultList(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('search:backToPanel:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            return interaction.update({
                content: '',
                embeds: [searchPanelEmbed(st)],
                components: searchPanelComponents(guildId, userId, st),
            });
        }

        if (id.startsWith('search:home:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await interaction.update({
                content: '',
                embeds: [homeEmbed()],
                components: homeComponents(),
            });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        // Photo manager
        if (id.startsWith('ph:')) {
            const [, action, gid, ownerId, postId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);
            const post = cache.get(postId);
            if (!post) return interaction.reply({ ephemeral: true, content: 'データが見つかりません' });
            if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '写真の編集は投稿者のみです' });

            const pv = photoView.get(k) ?? { postId, idx: 0 };
            pv.postId = postId;

            const urls = imageUrls(post);
            const total = urls.length;

            if (action === 'prev' && total) pv.idx = (pv.idx - 1 + total) % total;
            if (action === 'next' && total) pv.idx = (pv.idx + 1) % total;

            if (action === 'add') {

                const waitPayload = {
                    content: '',
                    embeds: [photoAddWaitingEmbed(post.name)],
                    components: photoWaitingComponents(guildId, userId),
                };

                await interaction.update(waitPayload);

                awaitingPhoto.set(k, {
                    postId,
                    channelId: interaction.channelId,
                    guildId,
                    backTo: 'detail',
                    uiMessageRef: {
                        webhook: interaction.webhook,
                        messageId: interaction.message?.id,
                    },
                });

                return;
            }

            if (action === 'del') {
                const urls = imageUrls(post);
                if (!urls.length) {
                    return interaction.reply({ ephemeral: true, content: '写真がありません' });
                }

                const idx = Math.max(0, Math.min(urls.length - 1, Number(pv.idx) || 0));

                return interaction.update({
                    content: '',
                    embeds: [confirmPhotoDeleteEmbed(post, idx)],
                    components: confirmComponents('deletePhoto', guildId, ownerId, postId, String(idx)),
                });
            }

            if (action === 'delall') {
                const imgs = post.images ?? [];
                if (!imgs.length) {
                    return interaction.reply({ ephemeral: true, content: '写真がありません' });
                }

                return interaction.update({
                    content: '',
                    embeds: [confirmDeleteAllPhotosEmbed(post)],
                    components: confirmComponents(
                        'deleteAllPhotos',
                        guildId,
                        ownerId,
                        postId,
                        '0',
                        imgs.length > 1
                    ),
                });
            }

            if (action === 'back') {
                const nav = getDetailNavState(guildId, userId, postId);
                const fromMine = nav?.fromMine ?? cameFromMine(k, postId, mineState);
                const forceHomeBack = nav?.forceHomeBack ?? false;

                const { detail, components } = renderDetail(interaction, {
                    post,
                    guildId,
                    userId,
                    fromMine,
                    total: forceHomeBack ? 1 : (fromMine ? 1 : (searchState.get(k)?.results?.length || 1)),
                    forceHomeBack,
                });

                await interaction.update({ embeds: [detail], components });
                await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
                return;
            }

            photoView.set(k, pv);

            const urls2 = imageUrls(post);
            const newTotal = urls2.length;

            // idx補正（削除後とかで範囲外になるのを防ぐ）
            if (pv.idx >= newTotal) pv.idx = Math.max(0, newTotal - 1);

            const embed = new EmbedBuilder()
                .setTitle(`🖼 写真管理: ${post.name}`)
                .setDescription(newTotal ? `写真 ${pv.idx + 1}/${newTotal}` : '写真はありません')
                .setImage(newTotal ? urls2[pv.idx] : null);

            return interaction.update({
                embeds: [embed],
                components: photoManagerComponents(guildId, ownerId, postId, newTotal),
            });
        }

        if (id.startsWith('mine:filter:')) {
            const [, , filter, gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) {
                return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
            }

            const st = mineState.get(k);
            if (!st) {
                return interaction.reply({ ephemeral: true, content: '一覧がありません' });
            }

            if (!['all', 'visited', 'planned'].includes(filter)) {
                return interaction.reply({ ephemeral: true, content: 'フィルタが不正です' });
            }

            st.visitFilter = filter;
            st.page = 0;
            mineState.set(k, st);

            return renderMineList(interaction, guildId, userId, { update: true });
        }

        // Mine list paging/back
        if (id.startsWith('mine:prev:') || id.startsWith('mine:next:')) {
            const [, dir, gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const st = mineState.get(k);
            if (!st?.results?.length) return interaction.reply({ ephemeral: true, content: '一覧がありません' });

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);

            const filteredIds = st.results.filter(pid => {
                const p = cache.get(pid);
                if (!p) return false;
                return visitFilterMatch(st, p);
            });

            const pageSize = 9;
            const maxPage = Math.max(0, Math.ceil(filteredIds.length / pageSize) - 1);

            st.page = Number(st.page) || 0;
            st.page += dir === 'prev' ? -1 : +1;
            if (st.page < 0) st.page = 0;
            if (st.page > maxPage) st.page = maxPage;
            mineState.set(k, st);

            return renderMineList(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('mine:home:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await interaction.update({ embeds: [homeEmbed()], components: homeComponents() });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        if (id.startsWith('mine:back:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            await renderMineList(interaction, guildId, userId, { update: true });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        // User select menu (search filter user)
        if (interaction.isUserSelectMenu()) {
            const id = interaction.customId;
            if (!id.startsWith('search:userPick:')) return;

            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const pickedIds = (interaction.values ?? []).filter(Boolean);
            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            st.userIdFilter = pickedIds.length ? pickedIds : null;
            searchState.set(k, st);

            return interaction.update({
                content: '',
                embeds: [searchPanelEmbed(st)],
                components: searchPanelComponents(guildId, userId, st),
            });
        }

        // String select menu (mine list pick)
        if (interaction.isStringSelectMenu()) {
            const id = interaction.customId;
            // ===== 検索：都道府県ピック =====
            if (id.startsWith('search:prefPick:')) {
                const parts = id.split(':');
                const gid = parts[2];
                const ownerId = parts[3];
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const picked = interaction.values ?? [];
                const page = Number(parts[4] || 0);

                const st = searchState.get(k) ?? {
                    userIdFilter: null,
                    prefectureFilters: [],
                    keyword: '',
                    ratingFilters: [],
                    results: [],
                    page: 0
                };

                const { slice } = prefSlice(page);
                const current = new Set(st.prefectureFilters ?? []);

                // 今のページにある都道府県は一旦外す
                for (const pref of slice) {
                    current.delete(pref);
                }

                // 今回選んだものを追加
                for (const pref of picked) {
                    current.add(pref);
                }

                st.prefectureFilters = [...current];
                searchState.set(k, st);

                return interaction.update({
                    content: st.prefectureFilters.length
                        ? `都道府県を設定しました: ${st.prefectureFilters.join(' / ')}`
                        : '都道府県を解除しました',
                    embeds: [searchPanelEmbed(st)],
                    components: searchPanelComponents(guildId, userId, st),
                });
            }

            if (id.startsWith('create:prefPick:') || id.startsWith('edit:prefPick:')) {
                const parts = id.split(':');
                const mode = parts[0];
                const gid = parts[2];
                const ownerId = parts[3];

                if (interaction.guildId !== gid) {
                    return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
                }

                const picked = interaction.values?.[0] ?? '';
                const d = draftRating.get(k);

                if (!d || d.mode !== mode) {
                    return interaction.reply({ ephemeral: true, content: '途中状態がありません' });
                }

                d.prefecture = picked;
                draftRating.set(k, d);

                if (mode === 'create') {
                    return renderCreatePanel(interaction, guildId, userId, { update: true });
                }

                return renderEditPanel(interaction, guildId, userId, { update: true });
            }

            if (id.startsWith('search:pick:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const postId = interaction.values?.[0];
                if (!postId || postId === 'none') {
                    return interaction.reply({ ephemeral: true, content: '選択が不正です' });
                }

                await ensureCacheLoadedForGuild(interaction.guild);
                const cache = getGuildCache(guildId);
                const post = cache.get(postId);
                if (!post) {
                    return interaction.reply({ ephemeral: true, content: 'データが見つかりません' });
                }

                const { detail, components } = renderDetail(interaction, {
                    post,
                    guildId,
                    userId,
                    fromMine: false,
                    total: 1,
                    forceHomeBack: false,
                });

                return interaction.update({
                    content: '',
                    embeds: [detail],
                    components,
                });
            }

            if (!id.startsWith('mine:pick:')) return;

            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const postId = interaction.values?.[0];
            if (!postId || postId === 'none') return interaction.reply({ ephemeral: true, content: '選択が不正です' });

            await interaction.deferUpdate();

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);
            const post = cache.get(postId);
            if (!post) {
                return interaction.editReply({
                    content: 'データが見つかりません',
                    embeds: [],
                    components: homeComponents(),
                });
            }

            const { detail, components } = renderDetail(interaction, {
                post,
                guildId,
                userId,
                fromMine: true,
                total: 1,
                forceHomeBack: false,
            });

            await interaction.editReply({
                content: '',
                embeds: [detail],
                components,
            });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }
        // Modal submit
        if (interaction.isModalSubmit()) {
            const id = interaction.customId;

            if (id.startsWith('modalCreateComment:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'create') {
                    return interaction.reply({ ephemeral: true, content: '新規登録状態がありません' });
                }

                const comment = interaction.fields.getTextInputValue('comment')?.trim() ?? '';

                if (comment.length > 500) {
                    return interaction.reply({
                        ephemeral: true,
                        content: `コメントは500文字以内で入力してください（現在 ${comment.length}文字）`
                    });
                }

                d.comment = comment;
                draftRating.set(k, d);

                await interaction.deferReply({ ephemeral: true });

                const ok = await rerenderCreatePanelFromRef(interaction, guildId, userId);

                if (ok) {
                    try { await interaction.deleteReply(); } catch { }
                    return;
                }

                await interaction.editReply({
                    content: '',
                    embeds: [buildCreatePanelEmbed(d)],
                    components: createPanelComponents(guildId, userId, d),
                });

                const msg = await interaction.fetchReply().catch(() => null);
                if (msg?.id) {
                    addUiMessageId(guildId, userId, msg.id);
                    createPanelPromptRef.set(k, {
                        webhook: interaction.webhook,
                        messageId: msg.id,
                    });
                }
                return;
            }

            if (id.startsWith('modalCreateTags:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'create') {
                    return interaction.reply({ ephemeral: true, content: '新規登録状態がありません' });
                }

                d.tags = parseTags(interaction.fields.getTextInputValue('tags'));
                draftRating.set(k, d);

                await interaction.deferReply({ ephemeral: true });

                const ok = await rerenderCreatePanelFromRef(interaction, guildId, userId);

                if (ok) {
                    try { await interaction.deleteReply(); } catch { }
                    return;
                }

                await interaction.editReply({
                    content: '',
                    embeds: [buildCreatePanelEmbed(d)],
                    components: createPanelComponents(guildId, userId, d),
                });

                const msg = await interaction.fetchReply().catch(() => null);
                if (msg?.id) {
                    addUiMessageId(guildId, userId, msg.id);
                    createPanelPromptRef.set(k, {
                        webhook: interaction.webhook,
                        messageId: msg.id,
                    });
                }
                return;
            }

            if (id.startsWith('modalCreateUrl:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'create') {
                    return interaction.reply({ ephemeral: true, content: '新規登録状態がありません' });
                }

                d.url = interaction.fields.getTextInputValue('url')?.trim() ?? '';
                draftRating.set(k, d);

                await interaction.deferReply({ ephemeral: true });

                const ok = await rerenderCreatePanelFromRef(interaction, guildId, userId);

                if (ok) {
                    try { await interaction.deleteReply(); } catch { }
                    return;
                }

                await interaction.editReply({
                    content: '',
                    embeds: [buildCreatePanelEmbed(d)],
                    components: createPanelComponents(guildId, userId, d),
                });

                const msg = await interaction.fetchReply().catch(() => null);
                if (msg?.id) {
                    addUiMessageId(guildId, userId, msg.id);
                    createPanelPromptRef.set(k, {
                        webhook: interaction.webhook,
                        messageId: msg.id,
                    });
                }
            }

            if (id.startsWith('modalCreateMap:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'create') {
                    return interaction.reply({ ephemeral: true, content: '新規登録状態がありません' });
                }

                d.mapUrl = interaction.fields.getTextInputValue('mapUrl')?.trim() ?? '';
                draftRating.set(k, d);

                await interaction.deferReply({ ephemeral: true });

                const ok = await rerenderCreatePanelFromRef(interaction, guildId, userId);

                if (ok) {
                    try { await interaction.deleteReply(); } catch { }
                    return;
                }

                await interaction.editReply({
                    content: '',
                    embeds: [buildCreatePanelEmbed(d)],
                    components: createPanelComponents(guildId, userId, d),
                });

                const msg = await interaction.fetchReply().catch(() => null);
                if (msg?.id) {
                    addUiMessageId(guildId, userId, msg.id);
                    createPanelPromptRef.set(k, {
                        webhook: interaction.webhook,
                        messageId: msg.id,
                    });
                }
                return;
            }

            if (id.startsWith('modalCreateName:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'create') {
                    return interaction.reply({ ephemeral: true, content: '新規登録状態がありません' });
                }

                const name = interaction.fields.getTextInputValue('name')?.trim();
                if (!name) {
                    return interaction.reply({ ephemeral: true, content: 'お店の名前は必須です' });
                }

                d.name = name;
                draftRating.set(k, d);

                await interaction.deferReply({ ephemeral: true });

                const ok = await rerenderCreatePanelFromRef(interaction, guildId, userId);

                if (ok) {
                    try { await interaction.deleteReply(); } catch { }
                    return;
                }

                await interaction.editReply({
                    content: '',
                    embeds: [buildCreatePanelEmbed(d)],
                    components: createPanelComponents(guildId, userId, d),
                });

                const msg = await interaction.fetchReply().catch(() => null);
                if (msg?.id) {
                    addUiMessageId(guildId, userId, msg.id);
                    createPanelPromptRef.set(k, {
                        webhook: interaction.webhook,
                        messageId: msg.id,
                    });
                }
                return;
            }

            if (id.startsWith('modalEditName:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'edit') {
                    return interaction.reply({ ephemeral: true, content: '編集状態がありません' });
                }

                const name = interaction.fields.getTextInputValue('name')?.trim();
                if (!name) {
                    return interaction.reply({ ephemeral: true, content: 'お店の名前は必須です' });
                }

                d.name = name;
                draftRating.set(k, d);

                await interaction.deferReply({ ephemeral: true });

                const ok = await rerenderEditPanelFromRef(interaction, guildId, userId);

                if (ok) {
                    try { await interaction.deleteReply(); } catch { }
                    return;
                }

                await interaction.editReply({
                    content: '',
                    embeds: [buildEditPanelEmbed(d)],
                    components: editPanelComponents(guildId, userId, d),
                });

                const msg = await interaction.fetchReply().catch(() => null);
                if (msg?.id) {
                    addUiMessageId(guildId, userId, msg.id);
                    editPanelPromptRef.set(k, {
                        webhook: interaction.webhook,
                        messageId: msg.id,
                    });
                }
                return;
            }

            if (id.startsWith('modalEditComment:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'edit') {
                    return interaction.reply({ ephemeral: true, content: '編集状態がありません' });
                }

                const comment = interaction.fields.getTextInputValue('comment')?.trim() ?? '';

                if (comment.length > 500) {
                    return interaction.reply({
                        ephemeral: true,
                        content: `コメントは500文字以内で入力してください（現在 ${comment.length}文字）`
                    });
                }

                d.comment = comment;
                draftRating.set(k, d);

                await interaction.deferReply({ ephemeral: true });

                const ok = await rerenderEditPanelFromRef(interaction, guildId, userId);

                if (ok) {
                    try { await interaction.deleteReply(); } catch { }
                    return;
                }

                await interaction.editReply({
                    content: '',
                    embeds: [buildEditPanelEmbed(d)],
                    components: editPanelComponents(guildId, userId, d),
                });

                const msg = await interaction.fetchReply().catch(() => null);
                if (msg?.id) {
                    addUiMessageId(guildId, userId, msg.id);
                    editPanelPromptRef.set(k, {
                        webhook: interaction.webhook,
                        messageId: msg.id,
                    });
                }
            }

            if (id.startsWith('modalEditTags:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'edit') {
                    return interaction.reply({ ephemeral: true, content: '編集状態がありません' });
                }

                d.tags = parseTags(interaction.fields.getTextInputValue('tags'));
                draftRating.set(k, d);

                await interaction.deferReply({ ephemeral: true });

                const ok = await rerenderEditPanelFromRef(interaction, guildId, userId);

                if (ok) {
                    try { await interaction.deleteReply(); } catch { }
                    return;
                }

                await interaction.editReply({
                    content: '',
                    embeds: [buildEditPanelEmbed(d)],
                    components: editPanelComponents(guildId, userId, d),
                });

                const msg = await interaction.fetchReply().catch(() => null);
                if (msg?.id) {
                    addUiMessageId(guildId, userId, msg.id);
                    editPanelPromptRef.set(k, {
                        webhook: interaction.webhook,
                        messageId: msg.id,
                    });
                }
            }

            if (id.startsWith('modalEditUrl:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'edit') {
                    return interaction.reply({ ephemeral: true, content: '編集状態がありません' });
                }

                d.url = interaction.fields.getTextInputValue('url')?.trim() ?? '';
                draftRating.set(k, d);

                await interaction.deferReply({ ephemeral: true });

                const ok = await rerenderEditPanelFromRef(interaction, guildId, userId);

                if (ok) {
                    try { await interaction.deleteReply(); } catch { }
                    return;
                }

                await interaction.editReply({
                    content: '',
                    embeds: [buildEditPanelEmbed(d)],
                    components: editPanelComponents(guildId, userId, d),
                });

                const msg = await interaction.fetchReply().catch(() => null);
                if (msg?.id) {
                    addUiMessageId(guildId, userId, msg.id);
                    editPanelPromptRef.set(k, {
                        webhook: interaction.webhook,
                        messageId: msg.id,
                    });
                }
            }

            if (id.startsWith('modalEditMap:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'edit') {
                    return interaction.reply({ ephemeral: true, content: '編集状態がありません' });
                }

                d.mapUrl = interaction.fields.getTextInputValue('mapUrl')?.trim() ?? '';
                draftRating.set(k, d);

                await interaction.deferReply({ ephemeral: true });

                const ok = await rerenderEditPanelFromRef(interaction, guildId, userId);

                if (ok) {
                    try { await interaction.deleteReply(); } catch { }
                    return;
                }

                await interaction.editReply({
                    content: '',
                    embeds: [buildEditPanelEmbed(d)],
                    components: editPanelComponents(guildId, userId, d),
                });

                const msg = await interaction.fetchReply().catch(() => null);
                if (msg?.id) {
                    addUiMessageId(guildId, userId, msg.id);
                    editPanelPromptRef.set(k, {
                        webhook: interaction.webhook,
                        messageId: msg.id,
                    });
                }
                return;
            }

            if (id.startsWith('modalVisitedDate:')) {
                const [, gid, ownerId, mode] = id.split(':');

                if (interaction.guildId !== gid) {
                    return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
                }

                const d = draftRating.get(k);
                if (!d || d.mode !== mode) {
                    return interaction.reply({ ephemeral: true, content: '途中状態がありません' });
                }

                const raw = interaction.fields.getTextInputValue('visitedDate');
                const normalized = normalizeVisitedDate(raw);

                if (normalized === null) {
                    return interaction.reply({
                        ephemeral: true,
                        content: '行った日付は YYYY/MM/DD または YYYY-MM-DD 形式で入力してください',
                    });
                }

                d.visitedDate = normalized || '';
                draftRating.set(k, d);

                await interaction.deferReply({ ephemeral: true });

                if (mode === 'create') {
                    const ok = await rerenderCreatePanelFromRef(interaction, guildId, userId);

                    if (ok) {
                        try { await interaction.deleteReply(); } catch { }
                        return;
                    }

                    await interaction.editReply({
                        content: '',
                        embeds: [buildCreatePanelEmbed(d)],
                        components: createPanelComponents(guildId, userId, d),
                    });

                    const msg = await interaction.fetchReply().catch(() => null);
                    if (msg?.id) {
                        addUiMessageId(guildId, userId, msg.id);
                        createPanelPromptRef.set(k, {
                            webhook: interaction.webhook,
                            messageId: msg.id,
                        });
                    }
                    return;
                }

                if (mode === 'edit') {
                    const ok = await rerenderEditPanelFromRef(interaction, guildId, userId);

                    if (ok) {
                        try { await interaction.deleteReply(); } catch { }
                        return;
                    }

                    await interaction.editReply({
                        content: '',
                        embeds: [buildEditPanelEmbed(d)],
                        components: editPanelComponents(guildId, userId, d),
                    });

                    const msg = await interaction.fetchReply().catch(() => null);
                    if (msg?.id) {
                        addUiMessageId(guildId, userId, msg.id);
                        editPanelPromptRef.set(k, {
                            webhook: interaction.webhook,
                            messageId: msg.id,
                        });
                    }
                    return;
                }
            }

            if (id.startsWith('modalSearch:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const keyword = interaction.fields.getTextInputValue('keyword')?.trim() ?? '';

                const st = searchState.get(k) ?? {
                    userIdFilter: null,
                    prefectureFilters: [],
                    keyword: '',
                    ratingFilters: [],
                    results: [],
                    page: 0
                };
                st.keyword = keyword;
                searchState.set(k, st);

                await interaction.deferReply({ ephemeral: true });

                const ref = searchKeywordPromptRef.get(k);

                const updated = await editPromptRef(ref, {
                    content: '',
                    embeds: [searchPanelEmbed(st)],
                    components: searchPanelComponents(guildId, userId, st),
                });

                searchKeywordPromptRef.delete(k);

                if (updated) {
                    try {
                        await interaction.deleteReply();
                    } catch { }
                    return;
                }

                // 元メッセージ更新に失敗したときだけ新規reply
                await interaction.editReply({
                    content: '',
                    embeds: [searchPanelEmbed(st)],
                    components: searchPanelComponents(guildId, userId, st),
                });

                const sent = await interaction.fetchReply().catch(() => null);
                if (sent?.id) {
                    addUiMessageId(guildId, userId, sent.id);
                }
                return;
            }
        } // interaction.isModalSubmit() 終了
    } catch (e) {
        console.error(e);
        if (interaction.isRepliable()) {
            try {
                const gid = interaction.guildId;
                const uid = interaction.user?.id;

                let errMsg;
                if (interaction.deferred || interaction.replied) {
                    errMsg = await interaction.followUp({
                        ephemeral: true,
                        content: `エラー: ${e.message}`
                    });
                } else {
                    errMsg = await interaction.reply({
                        ephemeral: true,
                        content: `エラー: ${e.message}`,
                        fetchReply: true
                    });
                }

                if (gid && uid && errMsg?.id) {
                    addUiMessageId(gid, uid, errMsg.id);
                }
            } catch { }
        }
    }
});

// ====== 写真添付拾う（ユーザーが画像投稿したら追加） ======
client.on(Events.MessageCreate, async msg => {
    try {
        if (msg.author.bot) return;
        if (!msg.guildId) return;

        const k = keyOf(msg.guildId, msg.author.id);
        const wait = awaitingPhoto.get(k);
        if (!wait) return;

        if (wait.guildId !== msg.guildId) return;
        if (wait.channelId !== msg.channelId) return;

        // 複数画像を全部拾う
        const imgs = [...(msg.attachments?.values() ?? [])].filter(a => isImageAttachment(a));
        if (!imgs.length) return;

        const guild = msg.guild;
        await ensureCacheLoadedForGuild(guild);
        const cache = getGuildCache(msg.guildId);

        const post = cache.get(wait.postId);
        if (!post) {
            if (wait.interaction) {
                try { await wait.interaction.deleteReply(); } catch { }
            }
            awaitingPhoto.delete(k);
            return;
        }

        post.images = post.images ?? [];

        const dbCh = await getDbChannelForGuild(guild);

        for (const att of imgs) {
            const copied = await dbCh.send({
                content: `__IMG__:${post.id}:${nowIso()}`,
                files: [att.url],
            });
            const dbAtt = copied.attachments.first();
            if (dbAtt) {
                post.images.push({
                    url: dbAtt.url,
                    msgId: copied.id,
                    ts: copied.createdTimestamp,
                });
            }
        }

        post.updated_at = nowIso();

        const dbMsg = await dbCh.messages.fetch(post.id);
        await dbMsg.edit({ embeds: [buildPostEmbedForDb(post)] });

        // cache更新
        cache.set(post.id, post);

        // 写真追加後は詳細画面に戻す
        if (wait.uiMessageRef) {
            if (wait.backTo === 'home') {

                await editPromptRef(wait.uiMessageRef, {
                    content: `✅ **${post.name}** に写真を追加しました`,
                    embeds: [
                        confirmEmbed(
                            '📄 詳細を開きますか？',
                            `**${post.name}**`
                        )
                    ],
                    components: confirmComponents(
                        'openDetailAfterCreate',
                        msg.guildId,
                        msg.author.id,
                        post.id
                    ),
                });
            } else {
                const nav = getDetailNavState(msg.guildId, msg.author.id, post.id);
                const fromMine = nav?.fromMine ?? cameFromMine(k, post.id, mineState);
                const forceHomeBack = nav?.forceHomeBack ?? false;

                const member = msg.member ?? await msg.guild.members.fetch(msg.author.id).catch(() => null);

                const { detail, components } = renderDetail(
                    {
                        guild: msg.guild,
                        user: msg.author,
                        memberPermissions: member?.permissions ?? null,
                    },
                    {
                        post,
                        guildId: msg.guildId,
                        userId: msg.author.id,
                        fromMine,
                        total: forceHomeBack ? 1 : (fromMine ? 1 : (searchState.get(k)?.results?.length || 1)),
                        forceHomeBack,
                    }
                );

                await editPromptRef(wait.uiMessageRef, {
                    content: '',
                    embeds: [detail],
                    components,
                });
            }
        } else if (wait.interaction) {
            try { await wait.interaction.deleteReply(); } catch { }
        }

        awaitingPhoto.delete(k);

        // ユーザー投稿メッセージ消す（権限必要）
        try { await msg.delete(); } catch { }

    } catch (e) {
        console.error(e);
    }
});

client.on(Events.ClientReady, async () => {
    console.log(`Logged in as ${client.user.tag}`);

    try {
        await registerCommands();
        console.log('Commands registered');
    } catch (e) {
        console.error('registerCommands failed:', e);
    }

    console.log(`DB channel name per guild: #${DB_CHANNEL_NAME}`);
});

console.log("LOGIN START");
client.login(TOKEN).catch(e => {
    console.error('client.login failed:', e);
    process.exit(1);
});
