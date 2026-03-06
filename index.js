// index.js (ESM)
// package.json に "type": "module" がある前提

import express from "express";
import dotenv from 'dotenv';
dotenv.config();

process.on('unhandledRejection', (reason) => {
    console.error('unhandledRejection:', reason);
});
process.on('uncaughtException', (err) => {
    console.error('uncaughtException:', err);
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

// 新規/編集の「星→Modal」繋ぎ
// key: guildId:userId -> {
//   mode:'create'|'edit',
//   postId?:string|null,
//   visited:boolean,
//   rating:number|null,
//   channelId:string,
//   prefecture?:string,
//   visitedDate?:string
// }
const draftRating = new Map();

// ユーザーごとのephemeral UIメッセージ管理
// key: guildId:userId -> Set(messageId)
const uiMessages = new Map();

// 検索状態 key: guildId:userId
// { userIdFilter?:string[]|null, prefectureFilters?:string[], keyword?:string, results?:string[], idx?:number, ratingFilters?:number[] }
const searchState = new Map();

// 自分の記録（カード一覧）状態 key: guildId:userId
// { results?:string[], page?:number, visitedFilterVisited?:boolean, visitedFilterUnvisited?:boolean }
const mineState = new Map();

// 写真追加待ち key: guildId:userId
// { postId, channelId, guildId, expiresAt, uiMessageRef?, backTo?: 'home'|'detail' }
const awaitingPhoto = new Map();

// ephemeralプロンプトを消すための参照
// key: guildId:userId -> { webhook, messageId }
const visitedDatePromptRef = new Map();
const prefPromptRef = new Map();

// 写真ビュー状態 key: guildId:userId
// { postId, idx }
const photoView = new Map();

// ====== util ======
async function blankCurrentMessage(interaction) {
    try {
        if (!interaction?.message?.id) return;
        await interaction.webhook.editMessage(interaction.message.id, {
            content: ' ',
            embeds: [],
            components: [],
        });
    } catch (e) {
        console.error('blankCurrentMessage failed:', e);
    }
}

async function blankMessageById(interaction, messageId) {
    try {
        if (!messageId) return;
        await interaction.webhook.editMessage(messageId, {
            content: ' ',
            embeds: [],
            components: [],
        });
    } catch (e) {
        console.error('blankMessageById failed:', e);
    }
}

async function blankPromptRef(ref) {
    try {
        if (!ref?.webhook || !ref?.messageId) return;
        await ref.webhook.editMessage(ref.messageId, {
            content: ' ',
            embeds: [],
            components: [],
        });
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

async function clearEphemeralMessage(interaction) {
    try {
        if (interaction?.message?.id) {
            await interaction.webhook.editMessage(interaction.message.id, {
                content: ' ',
                embeds: [],
                components: [],
            });
        }
    } catch { }
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
        info.setFooter({ text: `更新: ${new Date(post.updated_at).toLocaleString()}` });
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

function nowIso() {
    return new Date().toISOString();
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
    const v = !!state?.visitedFilterVisited;
    const nv = !!state?.visitedFilterUnvisited;

    // 両方ON / 両方OFF は全件
    if ((v && nv) || (!v && !nv)) return true;

    if (v) return post.visited !== false;
    if (nv) return post.visited === false;

    return true;
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
    const ch = channels.find(c => c && c.isTextBased() && c.name?.toLowerCase() === DB_CHANNEL_NAME);
    if (!ch) throw new Error(`このサーバーに #${DB_CHANNEL_NAME} がありません作成してください`);
    return ch;
}

function buildPostEmbedForView(post, { sharedByUserId = null } = {}) {
    const lines = [];

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

    const fields = [
        { name: '🔗 Webサイト', value: post.url || '(なし)' },
        { name: '📍 場所', value: post.map_url || '(なし)' },
    ];

    const e = new EmbedBuilder()
        .setTitle(`🍽 ${post.name}`)
        .setDescription(lines.join('\n'))
        .addFields(fields);

    if (post.updated_at) {
        e.setFooter({ text: `更新: ${new Date(post.updated_at).toLocaleString()}` });
    }

    const urls = imageUrls(post);
    const mainImage = urls.length ? urls[urls.length - 1] : null;
    if (mainImage) e.setImage(mainImage);

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
        .setFooter({ text: `ID: ${post.id}  更新: ${new Date(post.updated_at).toLocaleString()}` });

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

// ====== UI builders ======
function photoWaitingEmbed(name) {
    return new EmbedBuilder()
        .setTitle('📷 写真追加')
        .setDescription(
            `**${name}** を登録しました\n` +
            'このチャンネルに写真を送信してください（5分以内）\n' +
            '送信した画像をすべて追加します投稿後、Botが元メッセージを消します'
        );
}

function photoAddWaitingEmbed(name) {
    return new EmbedBuilder()
        .setTitle('📷 写真追加')
        .setDescription(
            `**${name}** に写真を追加します\n` +
            'このチャンネルに写真を送信してください（5分以内）\n' +
            '送信した画像をすべて追加します投稿後、Botが元メッセージを消します'
        );
}

function photoWaitingComponents(guildId, userId) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId('photo:waiting')
                .setLabel('写真を送信待ち')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(true)
        ),
        ...cancelRow(guildId, userId),
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
        ...cancelRow(guildId, userId),
    ];
}

function buildVisitedDateModal(gid, ownerId, mode, postId = '', currentValue = '') {
    const modal = new ModalBuilder()
        .setCustomId(`modalVisitedDate:${gid}:${ownerId}:${mode}:${postId || ''}`)
        .setTitle('📅 訪問日（任意）');

    modal.addComponents(
        new ActionRowBuilder().addComponents(
            new TextInputBuilder()
                .setCustomId('visitedDate')
                .setLabel('訪問日（任意）')
                .setStyle(TextInputStyle.Short)
                .setRequired(false)
                .setPlaceholder('2026/03/06 または 2026-03-06')
                .setValue(currentValue ?? '')
        )
    );

    return modal;
}

function visitedDateAskComponents(guildId, userId, mode, postId = '') {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`date:input:${guildId}:${userId}:${mode}:${postId}`)
                .setLabel('📅 訪問日を入力')
                .setStyle(ButtonStyle.Primary),
            new ButtonBuilder()
                .setCustomId(`date:skip:${guildId}:${userId}:${mode}:${postId}`)
                .setLabel('スキップ')
                .setStyle(ButtonStyle.Secondary)
        ),
    ];
}

function buildCreateModal(gid, ownerId) {
    const modal = new ModalBuilder().setCustomId(`modalCreate:${gid}:${ownerId}`).setTitle('➕ 新規記録');
    modal.addComponents(
        new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('name').setLabel('店名').setStyle(TextInputStyle.Short).setRequired(true)
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('comment').setLabel('コメント').setStyle(TextInputStyle.Paragraph).setRequired(false)
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('url').setLabel('Webサイト').setStyle(TextInputStyle.Short).setRequired(false)
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('mapUrl').setLabel('📍 GoogleMapリンク').setStyle(TextInputStyle.Short).setRequired(false)
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder()
                .setCustomId('tags')
                .setLabel('タグ（カンマ区切り）')
                .setStyle(TextInputStyle.Short)
                .setRequired(false)
                .setPlaceholder('ラーメン, デート, 深夜')
        )
    );
    return modal;
}

function buildEditModal(gid, ownerId, postId, post) {
    const modal = new ModalBuilder().setCustomId(`modalEdit:${gid}:${ownerId}:${postId}`).setTitle('✏ 編集');
    modal.addComponents(
        new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('name').setLabel('店名').setStyle(TextInputStyle.Short).setRequired(true).setValue(post.name ?? '')
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('comment').setLabel('コメント').setStyle(TextInputStyle.Paragraph).setRequired(false).setValue(post.comment ?? '')
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('url').setLabel('Webサイト').setStyle(TextInputStyle.Short).setRequired(false).setValue(post.url ?? '')
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('mapUrl').setLabel('📍 GoogleMapリンク').setStyle(TextInputStyle.Short).setRequired(false).setValue(post.map_url ?? '')
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder()
                .setCustomId('tags')
                .setLabel('タグ（カンマ区切り）')
                .setStyle(TextInputStyle.Short)
                .setRequired(false)
                .setPlaceholder('ラーメン, デート, 深夜')
                .setValue((post.tags ?? []).join(', '))
        )
    );
    return modal;
}

async function openCreateOrEditModal(interaction, { mode, gid, ownerId, guildId, draft }) {
    if (mode === 'create') {
        return interaction.showModal(buildCreateModal(gid, ownerId));
    }

    const postId = draft?.postId;
    await ensureCacheLoadedForGuild(interaction.guild);
    const cache = getGuildCache(guildId);
    const post = cache.get(postId);

    if (!post) {
        return interaction.reply({ ephemeral: true, content: '対象データが見つかりません' });
    }

    if (!canEdit(interaction, post)) {
        return interaction.reply({ ephemeral: true, content: '編集できません（投稿者のみ）' });
    }

    return interaction.showModal(buildEditModal(gid, ownerId, postId, post));
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

function cancelRow(guildId, userId) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`cancel:flow:${guildId}:${userId}`)
                .setLabel('✖ 中断')
                .setStyle(ButtonStyle.Danger)
        ),
    ];
}

function withCancelRows(rows, guildId, userId) {
    return [...rows, ...cancelRow(guildId, userId)];
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

        new ButtonBuilder()
            .setCustomId(`${mode}:prefSkip:${guildId}:${userId}`)
            .setLabel('スキップ（未設定）で次へ')
            .setStyle(ButtonStyle.Secondary)
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
    const keywordLabel = state.keyword ? `「${state.keyword}」` : '指定なし';
    const ratingLabel = state.ratingFilters?.length
        ? state.ratingFilters.map(r => `⭐${r}`).join(' ')
        : '指定なし';

    return new EmbedBuilder()
        .setTitle('🔎 検索条件')
        .setDescription(
            `👤 人: ${userLabel}\n` +
            `🗾 都道府県: ${prefLabel}\n` +
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
                .setLabel('👤 人')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`search:setPref:${guildId}:${userId}`)
                .setLabel('🗾 都道府県')
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

function photoManagerComponents(guildId, userId, postId, hasAny) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId(`ph:prev:${guildId}:${userId}:${postId}`).setLabel('◀').setStyle(ButtonStyle.Secondary).setDisabled(!hasAny),
            new ButtonBuilder().setCustomId(`ph:next:${guildId}:${userId}:${postId}`).setLabel('▶').setStyle(ButtonStyle.Secondary).setDisabled(!hasAny),
            new ButtonBuilder().setCustomId(`ph:add:${guildId}:${userId}:${postId}`).setLabel('➕ 追加').setStyle(ButtonStyle.Primary)
        ),
        new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId(`ph:del:${guildId}:${userId}:${postId}`).setLabel('🗑 この写真削除').setStyle(ButtonStyle.Danger).setDisabled(!hasAny),
            new ButtonBuilder().setCustomId(`ph:delall:${guildId}:${userId}:${postId}`).setLabel('🗑 すべて削除').setStyle(ButtonStyle.Danger).setDisabled(!hasAny),
            new ButtonBuilder()
                .setCustomId(`ph:back:${guildId}:${userId}:${postId}`)
                .setLabel('🔙 戻る')
                .setStyle(ButtonStyle.Secondary)
        ),
    ];
}

function confirmComponents(kind, guildId, userId, postId, extra = '') {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`confirm:yes:${kind}:${guildId}:${userId}:${postId}:${extra}`)
                .setLabel('はい')
                .setStyle(ButtonStyle.Danger),
            new ButtonBuilder()
                .setCustomId(`confirm:no:${kind}:${guildId}:${userId}:${postId}:${extra}`)
                .setLabel('いいえ')
                .setStyle(ButtonStyle.Secondary)
        ),
    ];
}

function confirmEmbed(title, message) {
    return new EmbedBuilder()
        .setTitle(title)
        .setDescription(message);
}

function mineListComponents(guildId, userId, page, hasPrev, hasNext, options, st) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId(`mine:toggleVisited:${guildId}:${userId}`).setLabel(`${st?.visitedFilterVisited ? '☑' : '☐'} 行った`).setStyle(ButtonStyle.Secondary),
            new ButtonBuilder().setCustomId(`mine:toggleUnvisited:${guildId}:${userId}`).setLabel(`${st?.visitedFilterUnvisited ? '☑' : '☐'} 行きたい`).setStyle(ButtonStyle.Secondary),
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

    const pageSize = 5;
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
        .setDescription(`一覧（${filteredIds.length ? start + 1 : 0}-${start + slice.length} / ${filteredIds.length}）`);

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

function detailActionComponents(guildId, userId, postId, { fromMine = false, canEditThis = true, total = 1 } = {}) {
    const row1 = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId(`res:prev:${guildId}:${userId}`)
            .setLabel('◀')
            .setStyle(ButtonStyle.Secondary)
            .setDisabled(fromMine || total <= 1),

        new ButtonBuilder()
            .setCustomId(`res:next:${guildId}:${userId}`)
            .setLabel('▶')
            .setStyle(ButtonStyle.Secondary)
            .setDisabled(fromMine || total <= 1),

        new ButtonBuilder()
            .setCustomId(`res:share:${guildId}:${userId}:${postId}`)
            .setLabel('📤 このチャンネルに送信')
            .setStyle(ButtonStyle.Primary)
    );

    const row2 = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId(`res:edit:${guildId}:${userId}:${postId}`)
            .setLabel('✏ 編集')
            .setStyle(ButtonStyle.Secondary)
            .setDisabled(!canEditThis),

        new ButtonBuilder()
            .setCustomId(`res:photos:${guildId}:${userId}:${postId}`)
            .setLabel('🖼 写真管理')
            .setStyle(ButtonStyle.Secondary)
            .setDisabled(!canEditThis),

        new ButtonBuilder()
            .setCustomId(`res:delete:${guildId}:${userId}:${postId}`)
            .setLabel('🗑 削除')
            .setStyle(ButtonStyle.Danger)
            .setDisabled(!canEditThis),

        new ButtonBuilder()
            .setCustomId(fromMine ? `mine:back:${guildId}:${userId}` : `res:back:${guildId}:${userId}`)
            .setLabel('🔙 戻る')
            .setStyle(ButtonStyle.Secondary)
    );

    return [row1, row2];
}

function cameFromMine(k, postId, mineState) {
    const mine = mineState.get(k);
    return !!mine?.results?.includes(postId);
}

function renderDetail(interaction, { post, guildId, userId, fromMine, total = 1 }) {
    const detail = buildPostEmbedForView(post);
    detail.setTitle(`📄 詳細  ${post.name}`.trim());

    const components = detailActionComponents(guildId, userId, post.id, {
        fromMine,
        canEditThis: canEdit(interaction, post),
        total,
    });

    return { detail, components };
}

async function renderSearchResult(interaction, guildId, userId, { update = false } = {}) {
    const k = keyOf(guildId, userId);
    const st = searchState.get(k);
    if (!st?.results?.length) {
        const panel = searchState.get(k) ?? {
            userIdFilter: null,
            prefectureFilters: [],
            keyword: '',
            ratingFilters: [],
            results: [],
            idx: 0
        };
        const payload = {
            embeds: [searchPanelEmbed(panel)],
            components: searchPanelComponents(guildId, userId, panel)
        };
        if (update) return updateLike(interaction, payload);

        await interaction.reply({ ephemeral: true, ...payload });
        await rememberUiReply(interaction, guildId, userId);
        return;
    }

    await ensureCacheLoadedForGuild(interaction.guild);
    const cache = getGuildCache(guildId);

    const idx = Math.max(0, Math.min(st.results.length - 1, Number(st.idx) || 0));
    st.idx = idx;
    searchState.set(k, st);

    const postId = st.results[idx];
    const post = cache.get(postId);
    if (!post) {
        // 消えてたら再描画で回避
        st.results = st.results.filter(x => x !== postId);
        st.idx = 0;
        searchState.set(k, st);
        return renderSearchResult(interaction, guildId, userId, { update });
    }

    const embed = buildPostEmbedForView(post);
    embed.setTitle(`🔎 検索結果 (${idx + 1}/${st.results.length})  ${post.name}`.trim());

    const payload = {
        embeds: [embed],
        components: detailActionComponents(guildId, userId, postId, {
            fromMine: false,
            canEditThis: canEdit(interaction, post),
            total: st.results.length,
        }),
    };

    if (update) return updateLike(interaction, payload);

    await interaction.reply({ ephemeral: true, ...payload });
    await rememberUiReply(interaction, guildId, userId);
    return;
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

            try {
                await ensureCacheLoadedForGuild(interaction.guild);
            } catch (e) {
                return interaction.reply({
                    ephemeral: true,
                    content: `エラー: ${e.message}\nこのサーバーに **#${DB_CHANNEL_NAME}** を作ってください`,
                });
            }

            await interaction.reply({ ephemeral: true, embeds: [homeEmbed()], components: homeComponents() });
            await rememberUiReply(interaction, guildId, userId);
            return;
        }

        // Buttons
        if (interaction.isButton()) {
            const id = interaction.customId;

            if (id.startsWith('photo:skip:')) {
                const [, , gid, ownerId] = id.split(':');

                if (interaction.guildId !== gid) {
                    return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
                }

                awaitingPhoto.delete(k);

                try {
                    await interaction.webhook.deleteMessage(interaction.message.id);
                } catch {
                    try {
                        await interaction.update({
                            content: ' ',
                            embeds: [],
                            components: [],
                        });
                    } catch { }
                }

                return;
            }

            // 記録フロー中断
            if (id.startsWith('cancel:flow:')) {
                const [, , gid, ownerId] = id.split(':');

                if (interaction.guildId !== gid) {
                    return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
                }

                draftRating.delete(k);
                visitedDatePromptRef.delete(k);
                prefPromptRef.delete(k);

                const wait = awaitingPhoto.get(k);
                awaitingPhoto.delete(k);

                if (wait?.backTo === 'detail' && wait.postId) {
                    await ensureCacheLoadedForGuild(interaction.guild);
                    const cache = getGuildCache(guildId);
                    const post = cache.get(wait.postId);

                    if (post) {
                        const fromMine = cameFromMine(k, wait.postId, mineState);

                        const { detail, components } = renderDetail(interaction, {
                            post,
                            guildId,
                            userId,
                            fromMine,
                            total: fromMine ? 1 : (searchState.get(k)?.results?.length || 1),
                        });

                        return interaction.update({
                            content: '',
                            embeds: [detail],
                            components,
                        });
                    }
                }

                await interaction.update({
                    content: '',
                    embeds: [homeEmbed()],
                    components: homeComponents(),
                });

                await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
                return;
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

                if (answer === 'no') {
                    if (!post) {
                        return interaction.update({
                            embeds: [homeEmbed()],
                            components: homeComponents(),
                        });
                    }

                    if (kind === 'deletePhoto' || kind === 'deleteAllPhotos') {
                        const urls = imageUrls(post);
                        const idx = Math.max(0, Math.min(urls.length - 1, Number(extra) || 0));
                        photoView.set(k, { postId, idx });

                        const embed = new EmbedBuilder()
                            .setTitle(`🖼 写真管理: ${post.name}`)
                            .setDescription(urls.length ? `写真 ${idx + 1}/${urls.length}` : '写真はありません')
                            .setImage(urls.length ? urls[idx] : null);

                        return interaction.update({
                            embeds: [embed],
                            components: photoManagerComponents(guildId, ownerId, postId, urls.length > 0),
                        });
                    }

                    const fromMine = cameFromMine(k, postId, mineState);
                    const { detail, components } = renderDetail(interaction, {
                        post,
                        guildId,
                        userId,
                        fromMine,
                        total: fromMine ? 1 : (searchState.get(k)?.results?.length || 1),
                    });

                    return interaction.update({
                        content: '',
                        embeds: [detail],
                        components,
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

                    if (interaction.user.id !== post.created_by) {
                        return interaction.reply({ ephemeral: true, content: '削除できるのは登録者のみです' });
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
                        srch.idx = 0;
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
                        return renderSearchResult(interaction, guildId, userId, { update: true });
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
                        components: photoManagerComponents(guildId, ownerId, postId, urls.length > 0),
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

            if (id.startsWith('date:skip:')) {
                const [, action, gid, ownerId, mode, postId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== mode) {
                    return interaction.reply({ ephemeral: true, content: '途中状態がありません最初からやり直して' });
                }

                d.visitedDate = '';
                d.prefPromptMessageId = interaction.message?.id ?? null;
                draftRating.set(k, d);

                await interaction.update({
                    content: '都道府県を選択してください（任意）',
                    embeds: [],
                    components: withCancelRows(
                        prefPickComponents(mode, gid, ownerId),
                        gid,
                        ownerId
                    ),
                });

                prefPromptRef.set(k, {
                    webhook: interaction.webhook,
                    messageId: interaction.message?.id,
                });

                return;
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
                    idx: 0
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
                    new ButtonBuilder().setCustomId(`search:prefPagePrev:${guildId}:${ownerId}:${p}`).setLabel('◀ 前へ').setStyle(ButtonStyle.Secondary).setDisabled(p <= 0),
                    new ButtonBuilder().setCustomId(`search:prefPageNext:${guildId}:${ownerId}:${p}`).setLabel('次へ ▶').setStyle(ButtonStyle.Secondary).setDisabled(p >= totalPages - 1),
                    new ButtonBuilder().setCustomId(`search:prefPageClear:${guildId}:${ownerId}`).setLabel('解除').setStyle(ButtonStyle.Secondary),
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
                    idx: 0
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
                    components: withCancelRows(
                        prefPickComponents(mode, gid, ownerId, next),
                        gid,
                        ownerId
                    ),
                });

                return;
            }

            if (id.startsWith('create:prefSkip:') || id.startsWith('edit:prefSkip:')) {
                const [mode, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== mode) {
                    return interaction.reply({ ephemeral: true, content: '途中状態がありません最初からやり直して' });
                }

                d.prefecture = '';
                d.prefPromptMessageId = interaction.message?.id ?? null;
                draftRating.set(k, d);

                prefPromptRef.set(k, {
                    webhook: interaction.webhook,
                    messageId: interaction.message?.id,
                });

                return openCreateOrEditModal(interaction, {
                    mode,
                    gid,
                    ownerId,
                    guildId,
                    draft: d,
                });
            }

            // Home
            if (id === 'home:create') {
                draftRating.delete(k);

                await interaction.update({
                    content: '訪問状態を選択してください',
                    embeds: [],
                    components: withCancelRows(visitRow('visitCreate', guildId, userId), guildId, userId),
                });
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
                    idx: 0
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
                    visitedFilterVisited: true,
                    visitedFilterUnvisited: true,
                });

                await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
                return renderMineList(interaction, guildId, userId, { update: true });
            }

            if (id.startsWith('visitCreate:') || id.startsWith('visitEdit:')) {
                const [prefix, kind, gid, ownerId, postId] = id.split(':');

                if (interaction.guildId !== gid) {
                    return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
                }

                const visited = kind === 'visited';
                const mode = prefix === 'visitCreate' ? 'create' : 'edit';

                let existingVisitedDate = '';
                let existingPrefecture = '';

                if (mode === 'edit' && postId) {
                    await ensureCacheLoadedForGuild(interaction.guild);
                    const cache = getGuildCache(guildId);
                    const oldPost = cache.get(postId);
                    existingVisitedDate = oldPost?.visited_date ?? '';
                    existingPrefecture = oldPost?.prefecture ?? '';
                }

                draftRating.set(k, {
                    mode,
                    postId: mode === 'edit' ? (postId || null) : null,
                    visited,
                    rating: null,
                    prefecture: existingPrefecture,
                    visitedDate: existingVisitedDate,
                    channelId: interaction.channelId,
                    visitedDatePromptMessageId: null,
                    prefPromptMessageId: !visited ? (interaction.message?.id ?? null) : null,
                });

                if (!visited) {
                    const cur = draftRating.get(k) ?? {};
                    cur.prefPromptMessageId = interaction.message?.id ?? null;
                    draftRating.set(k, cur);

                    await interaction.update({
                        content: '都道府県を選択してください（任意）',
                        embeds: [],
                        components: withCancelRows(
                            prefPickComponents(mode, gid, ownerId),
                            gid,
                            ownerId
                        ),
                    });

                    prefPromptRef.set(k, {
                        webhook: interaction.webhook,
                        messageId: interaction.message?.id,
                    });

                    return;
                }
                await interaction.update({
                    content: '評価を選択してください',
                    embeds: [],
                    components: withCancelRows(
                        ratingRow(mode === 'create' ? 'rateCreate' : 'rateEdit', guildId, ownerId, postId || ''),
                        guildId,
                        ownerId
                    ),
                });
                return;
            }

            // rating
            if (id.startsWith('rateCreate:') || id.startsWith('rateEdit:')) {
                const [prefix, ratingStr, gid, ownerId, postId] = id.split(':');

                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const rating = Number(ratingStr);
                if (!(rating >= 1 && rating <= 5)) return interaction.reply({ ephemeral: true, content: '評価が不正です' });

                const mode = (prefix === 'rateCreate') ? 'create' : 'edit';

                const prevDraft = draftRating.get(k);
                if (!prevDraft) {
                    return interaction.reply({ ephemeral: true, content: '先に「行った / 行きたい」を選んでください' });
                }

                draftRating.set(k, {
                    mode,
                    postId: mode === 'edit' ? (postId || null) : null,
                    visited: prevDraft.visited,
                    rating,
                    prefecture: prevDraft.prefecture ?? '',
                    visitedDate: prevDraft.visitedDate ?? '',
                    channelId: interaction.channelId,
                    visitedDatePromptMessageId: interaction.message?.id ?? null,
                });

                await interaction.update({
                    content: '訪問日を入力してください（任意）',
                    embeds: [],
                    components: withCancelRows(
                        visitedDateAskComponents(guildId, ownerId, mode, postId || ''),
                        guildId,
                        ownerId
                    ),
                });

                visitedDatePromptRef.set(k, {
                    webhook: interaction.webhook,
                    messageId: interaction.message?.id,
                });

                return;
            }

            // Search panel
            if (id.startsWith('search:setUser:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const row = new ActionRowBuilder().addComponents(
                    new UserSelectMenuBuilder()
                        .setCustomId(`search:userPick:${guildId}:${ownerId}`)
                        .setPlaceholder('登録者を選択してください（複数可）')
                        .setMinValues(0)
                        .setMaxValues(25)
                );

                await interaction.update({
                    content: '検索する登録者を選択してください',
                    embeds: [],
                    components: [row],
                });

                return;
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
                    idx: 0
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
                    idx: 0
                };
                const modal = new ModalBuilder().setCustomId(`modalSearch:${guildId}:${ownerId}`).setTitle('🔎 検索条件');

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

                const st = { userIdFilter: null, prefectureFilters: [], keyword: '', ratingFilters: [], results: [], idx: 0 };
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
                    idx: 0
                };
                const keywords = (st.keyword ?? '')
                    .toLowerCase()
                    .trim()
                    .split(/\s+/)   // 半角スペース区切り
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
                                .toLowerCase();

                            // 全キーワード一致
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
                st.idx = 0;
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

                return renderSearchResult(interaction, guildId, userId, { update: true });
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
                    idx: 0
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

            // Search result nav
            if (id.startsWith('res:prev:') || id.startsWith('res:next:')) {
                const [, dir, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const st = searchState.get(k);
                if (!st?.results?.length) return interaction.reply({ ephemeral: true, content: '結果がありません' });

                const delta = dir === 'prev' ? -1 : +1;
                st.idx = (Number(st.idx) || 0) + delta;
                if (st.idx < 0) st.idx = st.results.length - 1;
                if (st.idx >= st.results.length) st.idx = 0;
                searchState.set(k, st);

                return renderSearchResult(interaction, guildId, userId, { update: true });
            }

            // Share
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

                const fromMine = cameFromMine(k, postId, mineState);
                const { detail, components } = renderDetail(interaction, {
                    post: fresh,
                    guildId,
                    userId,
                    fromMine,
                    total: fromMine ? 1 : (searchState.get(k)?.results?.length || 1),
                });

                await interaction.update({
                    content: '📤 このチャンネルに送信しました',
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

                await interaction.update({
                    content: `訪問状態を選択してください（現在: ${visitLabel(post)}）`,
                    embeds: [],
                    components: withCancelRows(
                        visitRow('visitEdit', guildId, ownerId, postId),
                        guildId,
                        ownerId
                    ),
                });
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
                    components: photoManagerComponents(guildId, ownerId, postId, total > 0),
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
                if (interaction.user.id !== post.created_by) {
                    return interaction.reply({ ephemeral: true, content: '削除できるのは登録者のみです' });
                }

                return interaction.update({
                    embeds: [
                        confirmEmbed(
                            '⚠ 店情報を削除',
                            `**${post.name}** の記録と写真をすべて削除します。\n本当に削除しますか？`
                        )
                    ],
                    components: confirmComponents('deletePost', guildId, userId, postId),
                });
            }

            // Back from result -> search panel
            if (id.startsWith('res:back:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const st = searchState.get(k) ?? {
                    userIdFilter: null,
                    prefectureFilters: [],
                    keyword: '',
                    ratingFilters: [],
                    results: [],
                    idx: 0
                };

                await interaction.update({
                    embeds: [searchPanelEmbed(st)],
                    components: searchPanelComponents(guildId, userId, st),
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
                        expiresAt: Date.now() + 300_000,
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
                    if (!urls.length) return interaction.reply({ ephemeral: true, content: '写真がありません' });

                    return interaction.update({
                        embeds: [
                            confirmEmbed(
                                '⚠ 写真を削除',
                                '表示中のこの写真を削除しますか？'
                            )
                        ],
                        components: confirmComponents('deletePhoto', guildId, ownerId, postId, String(pv.idx)),
                    });
                }

                if (action === 'delall') {
                    const imgs = post.images ?? [];
                    if (!imgs.length) {
                        return interaction.reply({ ephemeral: true, content: '写真がありません' });
                    }

                    return interaction.update({
                        embeds: [
                            confirmEmbed(
                                '⚠ 写真をすべて削除',
                                `**${post.name}** の写真をすべて削除しますか？`
                            )
                        ],
                        components: confirmComponents('deleteAllPhotos', guildId, ownerId, postId),
                    });
                }

                if (action === 'back') {
                    const detail = buildPostEmbedForView(post);
                    detail.setTitle(`📄 詳細  ${post.name}`.trim());

                    const mine = mineState.get(k);
                    const fromMine = mine?.results?.includes(postId);

                    const components = detailActionComponents(guildId, userId, postId, {
                        fromMine,
                        canEditThis: canEdit(interaction, post),
                        total: fromMine ? 1 : 1,
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
                    components: photoManagerComponents(guildId, ownerId, postId, newTotal > 0),
                });
            }

            if (id.startsWith('mine:toggleVisited:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const st = mineState.get(k);
                if (!st) return interaction.reply({ ephemeral: true, content: '一覧がありません' });

                st.visitedFilterVisited = !st.visitedFilterVisited;
                st.page = 0;
                mineState.set(k, st);

                return renderMineList(interaction, guildId, userId, { update: true });
            }

            if (id.startsWith('mine:toggleUnvisited:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const st = mineState.get(k);
                if (!st) return interaction.reply({ ephemeral: true, content: '一覧がありません' });

                st.visitedFilterUnvisited = !st.visitedFilterUnvisited;
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

                const pageSize = 5;
                const maxPage = Math.max(0, Math.ceil(st.results.length / pageSize) - 1);

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
            return;
        }

        // User select menu (search filter user)
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
                idx: 0
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
                    idx: 0
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
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const picked = interaction.values?.[0] ?? '';
                const d = draftRating.get(k);
                if (!d || d.mode !== mode) {
                    return interaction.reply({ ephemeral: true, content: '途中状態がありません最初からやり直して' });
                }

                d.prefecture = picked;
                d.prefPromptMessageId = interaction.message?.id ?? null;
                draftRating.set(k, d);

                prefPromptRef.set(k, {
                    webhook: interaction.webhook,
                    messageId: interaction.message?.id,
                });

                return openCreateOrEditModal(interaction, {
                    mode,
                    gid,
                    ownerId,
                    guildId,
                    draft: d,
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

            const embed = buildPostEmbedForView(post);
            embed.setTitle(`📄 詳細  ${post.name}`.trim());

            await interaction.editReply({
                content: '',
                embeds: [embed],
                components: detailActionComponents(guildId, userId, postId, {
                    fromMine: true,
                    canEditThis: true,
                    total: 1,
                }),
            });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }
        // Modal submit
        if (interaction.isModalSubmit()) {
            const id = interaction.customId;

            if (id.startsWith('modalVisitedDate:')) {
                const [, gid, ownerId, mode, postId] = id.split(':');
                if (interaction.guildId !== gid) {
                    return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });
                }

                const d = draftRating.get(k);
                if (!d || d.mode !== mode) {
                    return interaction.reply({ ephemeral: true, content: '途中状態がありません最初からやり直して' });
                }

                const raw = interaction.fields.getTextInputValue('visitedDate');
                const normalized = normalizeVisitedDate(raw);

                if (normalized === null) {
                    return interaction.reply({
                        ephemeral: true,
                        content: '訪問日は YYYY/MM/DD または YYYY-MM-DD 形式で入力してください',
                    });
                }

                d.visitedDate = normalized || '';
                draftRating.set(k, d);

                await interaction.deferReply({ ephemeral: true });

                const visitedRef = visitedDatePromptRef.get(k);

                const updated = await editPromptRef(visitedRef, {
                    content: '都道府県を選択してください（任意）',
                    embeds: [],
                    components: withCancelRows(
                        prefPickComponents(mode, gid, ownerId),
                        gid,
                        ownerId
                    ),
                });

                if (updated) {
                    prefPromptRef.set(k, visitedRef);
                    d.prefPromptMessageId = visitedRef?.messageId ?? null;
                    draftRating.set(k, d);

                    visitedDatePromptRef.delete(k);

                    try {
                        await interaction.deleteReply();
                    } catch { }
                    return;
                }

                // 更新できなかったときだけ新しく出す
                await interaction.editReply({
                    content: '都道府県を選択してください（任意）',
                    embeds: [],
                    components: withCancelRows(
                        prefPickComponents(mode, gid, ownerId),
                        gid,
                        ownerId
                    ),
                });

                const sent = await interaction.fetchReply().catch(() => null);
                if (sent?.id) {
                    addUiMessageId(guildId, userId, sent.id);

                    prefPromptRef.set(k, {
                        webhook: interaction.webhook,
                        messageId: sent.id,
                    });

                    d.prefPromptMessageId = sent.id;
                    draftRating.set(k, d);
                }

                visitedDatePromptRef.delete(k);
                return;
            }

            // Create
            if (id.startsWith('modalCreate:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'create') {
                    return interaction.reply({ ephemeral: true, content: '評価が未選択です。もう一度やり直してください。' });
                }

                const prefRef = prefPromptRef.get(k);

                const name = interaction.fields.getTextInputValue('name')?.trim();
                const comment = interaction.fields.getTextInputValue('comment')?.trim();
                const url = interaction.fields.getTextInputValue('url')?.trim();
                const mapUrl = interaction.fields.getTextInputValue('mapUrl')?.trim();
                const tags = parseTags(interaction.fields.getTextInputValue('tags'));
                const pref = (d?.prefecture ?? '').trim();
                const visited = d.visited !== false;
                const visitedDate = visited ? (d.visitedDate ?? '') : '';

                if (!name) {
                    return interaction.reply({ ephemeral: true, content: '店名は必須です' });
                }

                // Modal submit は必ず応答する
                await interaction.deferReply({ ephemeral: true });

                const post = {
                    id: 'TEMP',
                    name,
                    visited,
                    rating: visited ? d.rating : null,
                    comment,
                    url,
                    map_url: mapUrl,
                    visited_date: visitedDate,
                    tags,
                    prefecture: pref,
                    images: [],
                    created_by: userId,
                    created_at: nowIso(),
                    updated_at: nowIso(),
                };

                await ensureCacheLoadedForGuild(interaction.guild);
                const cache = getGuildCache(guildId);

                const dbCh = await getDbChannelForGuild(interaction.guild);
                const sent = await dbCh.send({ embeds: [buildPostEmbedForDb({ ...post, id: '0' })] });

                post.id = sent.id;
                await sent.edit({ embeds: [buildPostEmbedForDb(post)] });
                cache.set(post.id, post);

                const waitPayload = {
                    content: '',
                    embeds: [photoWaitingEmbed(post.name)],
                    components: photoWaitingComponentsForCreate(guildId, userId),
                };

                draftRating.delete(k);
                visitedDatePromptRef.delete(k);
                prefPromptRef.delete(k);

                const updated = await editPromptRef(prefRef, waitPayload);

                let uiRef = prefRef ?? null;

                if (!updated) {
                    await interaction.editReply(waitPayload);
                    const replyMsg = await interaction.fetchReply().catch(() => null);

                    if (replyMsg?.id) {
                        addUiMessageId(guildId, userId, replyMsg.id);
                        uiRef = {
                            webhook: interaction.webhook,
                            messageId: replyMsg.id,
                        };
                    }
                } else {
                    try {
                        await interaction.deleteReply();
                    } catch { }
                }

                awaitingPhoto.set(k, {
                    postId: post.id,
                    channelId: interaction.channelId,
                    guildId,
                    expiresAt: Date.now() + 300_000,
                    backTo: 'home',
                    uiMessageRef: uiRef,
                });

                await clearOtherUiMessages(interaction, guildId, userId, uiRef?.messageId ?? null);
                return;
            }

            // Edit
            if (id.startsWith('modalEdit:')) {
                const [, gid, ownerId, postId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

                await ensureCacheLoadedForGuild(interaction.guild);
                const cache = getGuildCache(guildId);
                const post = cache.get(postId);
                if (!post) return interaction.reply({ ephemeral: true, content: '対象データが見つかりません' });
                if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '編集できません（投稿者のみ）' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'edit' || d.postId !== postId) {
                    return interaction.reply({ ephemeral: true, content: '評価選択が未完了ですもう一度やり直してください' });
                }

                await blankPromptRef(prefPromptRef.get(k));
                prefPromptRef.delete(k);

                const name = interaction.fields.getTextInputValue('name')?.trim();
                const comment = interaction.fields.getTextInputValue('comment')?.trim();
                const url = interaction.fields.getTextInputValue('url')?.trim();
                const tags = parseTags(interaction.fields.getTextInputValue('tags'));
                const mapUrl = interaction.fields.getTextInputValue('mapUrl')?.trim();
                const visited = d.visited !== false;
                const pref = (d?.prefecture ?? '').trim();
                const visitedDate = visited ? (d.visitedDate ?? '') : '';

                if (!name) {
                    return interaction.reply({ ephemeral: true, content: '店名は必須です' });
                }

                post.name = name;
                post.comment = comment;
                post.url = url;
                post.map_url = mapUrl;
                post.tags = tags;
                post.visited = visited;
                post.rating = visited ? d.rating : null;
                post.visited_date = visitedDate;
                post.prefecture = pref;
                post.updated_at = nowIso();

                const dbCh = await getDbChannelForGuild(interaction.guild);
                const dbMsg = await dbCh.messages.fetch(postId);
                await dbMsg.edit({ embeds: [buildPostEmbedForDb(post)] });

                cache.set(postId, post);

                const fromMine = cameFromMine(k, postId, mineState);
                const { detail, components } = renderDetail(interaction, { post, guildId, userId, fromMine });

                await interaction.reply({
                    ephemeral: true,
                    content: '✅ 更新しました',
                    embeds: [detail],
                    components,
                });

                const sent = await rememberUiReply(interaction, guildId, userId);

                if (sent?.id) {
                    setTimeout(async () => {
                        try {
                            await interaction.webhook.deleteMessage(sent.id);
                        } catch { }
                    }, 5000);
                }

                return;
            }

            // Search condition
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
                    idx: 0
                };
                st.keyword = keyword;
                searchState.set(k, st);

                await interaction.reply({
                    ephemeral: true,
                    embeds: [searchPanelEmbed(st)],
                    components: searchPanelComponents(guildId, userId, st),
                });
                await rememberUiReply(interaction, guildId, userId);
                return;
            }
        }
    } catch (e) {
        console.error(e);
        if (interaction.isRepliable()) {
            try {
                await interaction.reply({ ephemeral: true, content: `エラー: ${e.message}` });
                const gid = interaction.guildId;
                const uid = interaction.user?.id;
                if (gid && uid) {
                    await rememberUiReply(interaction, gid, uid);
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

        if (Date.now() > wait.expiresAt) {
            if (wait.interaction) {
                try { await wait.interaction.deleteReply(); } catch { }
            }
            awaitingPhoto.delete(k);
            return;
        }

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
                    content: `✅ 写真を追加しました: ${post.name}`,
                    embeds: [homeEmbed()],
                    components: homeComponents(),
                });
            } else {
                const detail = buildPostEmbedForView(post);
                detail.setTitle(`📄 詳細  ${post.name}`.trim());

                const fromMine = cameFromMine(k, post.id, mineState);

                await editPromptRef(wait.uiMessageRef, {
                    content: '',
                    embeds: [detail],
                    components: detailActionComponents(msg.guildId, msg.author.id, post.id, {
                        fromMine,
                        canEditThis: true,
                        total: fromMine ? 1 : (searchState.get(k)?.results?.length || 1),
                    }),
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

const app = express();
app.get("/", (req, res) => res.send("Bot is running"));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log("Web server running on port", PORT);
});
