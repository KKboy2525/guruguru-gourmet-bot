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
// key: guildId:userId -> { mode:'create'|'edit', postId?:string|null, rating:number, channelId:string, prefecture?:string }
const draftRating = new Map();

// 検索状態 key: guildId:userId
// { userIdFilter?:string|null, prefectureFilter?:string|null, keyword?:string, results?:string[], idx?:number }
const searchState = new Map();

// 自分の記録（カード一覧）状態 key: guildId:userId
// { results?:string[], page?:number }
const mineState = new Map();

// 写真追加待ち key: guildId:userId
// { postId, channelId, guildId, expiresAt, uiMessageId?:string }
const awaitingPhoto = new Map();

// 評価メッセージ保存
const ratingPromptInteraction = new Map();

// 都道府県ピッカーのメッセージを消す用
const prefPromptInteraction = new Map();

// 写真ビュー状態 key: guildId:userId
// { postId, idx }
const photoView = new Map();

// ====== util ======
function imageUrls(post) {
    const imgs = post.images ?? [];
    return imgs.map(x => (typeof x === 'string' ? x : x?.url)).filter(Boolean);
}

async function safeUpdate(interaction, payload) {
    // まだACKしてないなら、まず3秒制限を回避
    if (!interaction.deferred && !interaction.replied) {
        await interaction.deferUpdate();
    }
    // deferUpdate後は update じゃなく editReply
    return interaction.editReply(payload);
}

function buildDetailEmbedsChunks(post, { sharedByUserId = null } = {}) {

    const info = new EmbedBuilder()
        .setTitle(`🍽 ${post.name}`)
        .setDescription(
            `${stars(post.rating)}\n\n` +
            `${post.comment}\n\n` +
            `🗾 ${post.prefecture ? post.prefecture : '(未設定)'}\n` +
            `🏷 ${tagString(post.tags)}\n` +
            `👤 登録者 <@${post.created_by}>\n` +
            (sharedByUserId ? `📤 共有 <@${sharedByUserId}>\n` : '')
        )
        .addFields({ name: '🔗 URL', value: post.url || '(なし)' });

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

    // 最初のメッセージ（情報 + 最大9画像）
    chunks.push([info, ...imageEmbeds.slice(0, 9)]);

    // 残り画像
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
    if (!ch) throw new Error(`このサーバーに #${DB_CHANNEL_NAME} がありません。作成してください。`);
    return ch;
}

function buildPostEmbedForView(post, { sharedByUserId = null } = {}) {
    const e = new EmbedBuilder()
        .setTitle(`🍽 ${post.name}`)
        .setDescription(
            `${stars(post.rating)}\n\n` +
            `${post.comment}\n\n` +
            `🗾 ${post.prefecture ? post.prefecture : '(未設定)'}\n` +   // ★追加
            `🏷 ${tagString(post.tags)}\n` +
            `👤 登録者 <@${post.created_by}>\n` +
            (sharedByUserId ? `📤 共有 <@${sharedByUserId}>\n` : '')
        )
        .addFields({ name: '🔗 URL', value: post.url || '(なし)' });

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
        .addFields({ name: '🔗 URL', value: post.url || '(なし)' })
        .setFooter({ text: `ID: ${post.id}  更新: ${new Date(post.updated_at).toLocaleString()}` });

    // 詳細は最後の画像をメインに
    const urls = imageUrls(post);
    const mainImage = urls.length ? urls[urls.length - 1] : null;
    if (mainImage) e.setImage(mainImage);
    return e;
}

// 自分の記録一覧（カード）
function buildCardEmbed(post) {
    const e = new EmbedBuilder()
        .setTitle(`🍽 ${post.name}`)
        .setDescription(
            `${stars(post.rating)}\n` +
            `🗾 ${post.prefecture ? post.prefecture : '(未設定)'}\n` +  // ★追加
            `🏷 ${tagString(post.tags)}`
        );

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
    if (!dbCh?.isTextBased()) throw new Error(`#${DB_CHANNEL_NAME} はテキストチャンネルである必要があります。`);

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
            new TextInputBuilder().setCustomId('url').setLabel('URL').setStyle(TextInputStyle.Short).setRequired(false)
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('tags').setLabel('タグ（カンマ区切り）').setStyle(TextInputStyle.Short).setRequired(false)
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
            new TextInputBuilder().setCustomId('url').setLabel('URL').setStyle(TextInputStyle.Short).setRequired(false).setValue(post.url ?? '')
        ),
        new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('tags').setLabel('タグ（カンマ区切り）').setStyle(TextInputStyle.Short).setRequired(false).setValue((post.tags ?? []).join(', '))
        )
    );
    return modal;
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
        .setPlaceholder(`都道府県（任意） ${p + 1}/${totalPages}`)
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
    const userLabel = state.userIdFilter ? `<@${state.userIdFilter}>` : '(指定なし)';
    const prefLabel = state.prefectureFilter ? state.prefectureFilter : '(指定なし)';
    const keyword = state.keyword ? `"${state.keyword}"` : '(なし)';
    return new EmbedBuilder().setTitle('🔎 検索').setDescription(
        `👤 人フィルター: ${userLabel}\n` +
        `🗾 都道府県: ${prefLabel}\n` +
        `🔤 キーワード: ${keyword}`
    );
}

function searchPanelComponents(guildId, userId) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId(`search:setUser:${guildId}:${userId}`).setLabel('👤 人を選ぶ').setStyle(ButtonStyle.Secondary),
            new ButtonBuilder().setCustomId(`search:setPref:${guildId}:${userId}`).setLabel('🗾 都道府県を選ぶ').setStyle(ButtonStyle.Secondary),
            new ButtonBuilder().setCustomId(`search:setText:${guildId}:${userId}`).setLabel('🔤 キーワード入力').setStyle(ButtonStyle.Secondary)
        ),
        new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId(`search:run:${guildId}:${userId}`).setLabel('✅ 検索実行').setStyle(ButtonStyle.Primary),
            new ButtonBuilder().setCustomId(`search:clear:${guildId}:${userId}`).setLabel('🧹 クリア').setStyle(ButtonStyle.Secondary),
            new ButtonBuilder().setCustomId(`search:back:${guildId}:${userId}`).setLabel('🏠 戻る').setStyle(ButtonStyle.Secondary)
        ),
    ];
}

function postResultComponents(guildId, viewerId, postId, total, { canEditThis = true } = {}) {
    const row1 = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId(`res:prev:${guildId}:${viewerId}`).setLabel('◀').setStyle(ButtonStyle.Secondary).setDisabled(total <= 1),
        new ButtonBuilder().setCustomId(`res:next:${guildId}:${viewerId}`).setLabel('▶').setStyle(ButtonStyle.Secondary).setDisabled(total <= 1),
        new ButtonBuilder().setCustomId(`res:share:${guildId}:${viewerId}:${postId}`).setLabel('📤 このチャンネルに送信').setStyle(ButtonStyle.Primary)
    );
    const row2 = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId(`res:edit:${guildId}:${viewerId}:${postId}`)
            .setLabel('✏ 編集')
            .setStyle(ButtonStyle.Secondary)
            .setDisabled(!canEditThis),
        new ButtonBuilder()
            .setCustomId(`res:photos:${guildId}:${viewerId}:${postId}`)
            .setLabel('🖼 写真管理')
            .setStyle(ButtonStyle.Secondary)
            .setDisabled(!canEditThis),
        new ButtonBuilder().setCustomId(`res:back:${guildId}:${viewerId}`).setLabel('🔙 戻る').setStyle(ButtonStyle.Secondary)
    );
    return [row1, row2];
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
            new ButtonBuilder()
                .setCustomId(`ph:back:${guildId}:${userId}:${postId}`)
                .setLabel('🔙 戻る')
                .setStyle(ButtonStyle.Secondary)
        ),
    ];
}

function mineListComponents(guildId, userId, page, hasPrev, hasNext, options) {
    return [
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
    return new EmbedBuilder().setTitle('🍽 グルメ記録').setDescription('操作を選んでね');
}

async function renderMineList(interaction, guildId, userId, { update = false } = {}) {
    await ensureCacheLoadedForGuild(interaction.guild);
    const cache = getGuildCache(guildId);

    const k = keyOf(guildId, userId);
    const st = mineState.get(k);
    if (!st?.results?.length) {
        const e = new EmbedBuilder().setTitle('📚 自分の記録').setDescription('(まだありません)');
        if (update) return interaction.update({ embeds: [e], components: homeComponents() });
        return interaction.reply({ ephemeral: true, embeds: [e], components: homeComponents() });
    }

    const pageSize = 5;
    const page = Math.max(0, Number(st.page) || 0);
    const start = page * pageSize;
    const slice = st.results.slice(start, start + pageSize).map(pid => cache.get(pid)).filter(Boolean);

    const listHeader = new EmbedBuilder()
        .setTitle('📚 自分の記録')
        .setDescription(`一覧（${start + 1}-${start + slice.length} / ${st.results.length}）`);

    const embeds = [listHeader, ...slice.map(p => buildCardEmbed(p))];

    const options = slice.slice(0, 25).map(p => ({
        label: (p.name ?? '').slice(0, 100),
        description: `${stars(p.rating)}  ${tagString(p.tags)}`.slice(0, 100),
        value: p.id,
    }));

    const hasPrev = page > 0;
    const hasNext = start + pageSize < st.results.length;

    const comps = mineListComponents(guildId, userId, page, hasPrev, hasNext, options);

    if (update) {
        if (!interaction.deferred && !interaction.replied) {
            await interaction.deferUpdate();
        }
        return interaction.editReply({ embeds, components: comps });
    }
    return interaction.reply({ ephemeral: true, embeds, components: comps });
}

function detailComponentsFromMine(guildId, userId, postId) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`res:share:${guildId}:${userId}:${postId}`)
                .setLabel('📤 このチャンネルに送信')
                .setStyle(ButtonStyle.Primary),

            new ButtonBuilder()
                .setCustomId(`res:edit:${guildId}:${userId}:${postId}`)
                .setLabel('✏ 編集')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`res:photos:${guildId}:${userId}:${postId}`)
                .setLabel('🖼 写真管理')
                .setStyle(ButtonStyle.Secondary),

            new ButtonBuilder()
                .setCustomId(`mine:back:${guildId}:${userId}`)
                .setLabel('🔙 戻る')
                .setStyle(ButtonStyle.Secondary)
        ),
    ];
}

function cameFromMine(k, postId, mineState) {
    const mine = mineState.get(k);
    return !!mine?.results?.includes(postId);
}

function renderDetail(interaction, { post, guildId, userId, fromMine }) {
    const detail = buildPostEmbedForView(post);
    detail.setTitle(`📄 詳細  ${post.name}`.trim());

    const components = fromMine
        ? detailComponentsFromMine(guildId, userId, post.id)
        : postResultComponents(guildId, userId, post.id, 1, { canEditThis: canEdit(interaction, post) });

    return { detail, components };
}

async function renderSearchResult(interaction, guildId, userId, { update = false } = {}) {
    const k = keyOf(guildId, userId);
    const st = searchState.get(k);
    if (!st?.results?.length) {
        const panel = searchState.get(k) ?? { userIdFilter: null, keyword: '' };
        const payload = { embeds: [searchPanelEmbed(panel)], components: searchPanelComponents(guildId, userId) };
        if (update) return interaction.update(payload);
        return interaction.reply({ ephemeral: true, ...payload });
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
        components: postResultComponents(guildId, userId, postId, st.results.length, { canEditThis: canEdit(interaction, post) }),
    };

    if (update) return interaction.update(payload);
    return interaction.reply({ ephemeral: true, ...payload });
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
                    content: `エラー: ${e.message}\nこのサーバーに **#${DB_CHANNEL_NAME}** を作ってください。`,
                });
            }

            return interaction.reply({ ephemeral: true, embeds: [homeEmbed()], components: homeComponents() });
        }

        // Buttons
        if (interaction.isButton()) {
            const id = interaction.customId;
            if (id.startsWith('search:prefPagePrev:') || id.startsWith('search:prefPageNext:')) {
                const parts = id.split(':');
                const gid = parts[2];
                const ownerId = parts[3];
                const page = Number(parts[4] || 0);

                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const next = id.includes('prefPageNext') ? page + 1 : page - 1;
                const { p, totalPages, slice } = prefSlice(next);

                const select = new StringSelectMenuBuilder()
                    .setCustomId(`search:prefPick:${guildId}:${ownerId}:${p}`)
                    .setPlaceholder(`都道府県を選んでください ${p + 1}/${totalPages}`)
                    .setMinValues(0)
                    .setMaxValues(1)
                    .addOptions(slice.map(x => ({ label: x, value: x })));

                const nav = new ActionRowBuilder().addComponents(
                    new ButtonBuilder().setCustomId(`search:prefPagePrev:${guildId}:${ownerId}:${p}`).setLabel('◀ 前へ').setStyle(ButtonStyle.Secondary).setDisabled(p <= 0),
                    new ButtonBuilder().setCustomId(`search:prefPageNext:${guildId}:${ownerId}:${p}`).setLabel('次へ ▶').setStyle(ButtonStyle.Secondary).setDisabled(p >= totalPages - 1),
                    new ButtonBuilder().setCustomId(`search:prefPageClear:${guildId}:${ownerId}`).setLabel('解除').setStyle(ButtonStyle.Secondary),
                );

                return interaction.update({
                    content: '都道府県を選んでね',
                    components: [new ActionRowBuilder().addComponents(select), nav],
                });
            }

            if (id.startsWith('search:prefPageClear:')) {
                const parts = id.split(':');
                const gid = parts[2];
                const ownerId = parts[3];

                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const st = searchState.get(k) ?? { userIdFilter: null, prefectureFilter: null, keyword: '', results: [], idx: 0 };
                st.prefectureFilter = null;
                searchState.set(k, st);

                return interaction.update({
                    content: '都道府県フィルタを解除しました。',
                    embeds: [searchPanelEmbed(st)],
                    components: searchPanelComponents(guildId, userId),
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

                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const next = id.includes('prefPageNext') ? page + 1 : page - 1;

                // ★同じephemeralメッセージを更新する（増やさない）
                await interaction.update({
                    content: '都道府県を選んでください（任意）',
                    components: prefPickComponents(mode, gid, ownerId, next),
                });

                // この interaction でも deleteReply できるように保存（最新に更新）
                prefPromptInteraction.set(k, interaction);
                return;
            }

            if (id.startsWith('create:prefSkip:') || id.startsWith('edit:prefSkip:')) {
                const [mode, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const d = draftRating.get(k);
                if (!d || d.mode !== mode) {
                    return interaction.reply({ ephemeral: true, content: '途中状態がありません。最初からやり直して。' });
                }

                d.prefecture = '';
                draftRating.set(k, d);

                try { await interaction.message.delete(); } catch { }

                // 都道府県ピッカーのephemeralメッセージを消す
                const prevPref = prefPromptInteraction.get(k);
                if (prevPref) { try { await prevPref.deleteReply(); } catch { } prefPromptInteraction.delete(k); }

                if (mode === 'create') {
                    return interaction.showModal(buildCreateModal(gid, ownerId));
                } else {
                    const postId = d.postId;
                    await ensureCacheLoadedForGuild(interaction.guild);
                    const cache = getGuildCache(guildId);
                    const post = cache.get(postId);
                    if (!post) return interaction.reply({ ephemeral: true, content: '対象データが見つかりません。' });
                    if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '編集できません（投稿者のみ）。' });

                    return interaction.showModal(buildEditModal(gid, ownerId, postId, post));
                }
            }

            // Home
            if (id === 'home:create') {
                draftRating.delete(k);

                await interaction.reply({
                    ephemeral: true,
                    content: '評価を選んでね（新規）',
                    components: ratingRow('rateCreate', guildId, userId),
                });

                // 評価メッセージ保存
                ratingPromptInteraction.set(k, interaction);
                return;
            }

            if (id === 'home:search') {
                const st = searchState.get(k) ?? { userIdFilter: null, prefectureFilter: null, keyword: '', results: [], idx: 0 };
                searchState.set(k, st);
                return interaction.reply({
                    ephemeral: true,
                    embeds: [searchPanelEmbed(st)],
                    components: searchPanelComponents(guildId, userId),
                });
            }

            if (id === 'home:mine') {
                await ensureCacheLoadedForGuild(interaction.guild);
                const cache = getGuildCache(guildId);

                const mine = [...cache.values()]
                    .filter(p => p.created_by === userId)
                    .sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''));

                mineState.set(k, { results: mine.map(p => p.id), page: 0 });
                return renderMineList(interaction, guildId, userId, { update: false });
            }

            // rating
            if (id.startsWith('rateCreate:') || id.startsWith('rateEdit:')) {
                const prev = ratingPromptInteraction.get(k);
                if (prev) { try { await prev.deleteReply(); } catch { } ratingPromptInteraction.delete(k); }

                const [prefix, ratingStr, gid, ownerId, postId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const rating = Number(ratingStr);
                if (!(rating >= 1 && rating <= 5)) return interaction.reply({ ephemeral: true, content: '評価が不正です。' });

                const mode = (prefix === 'rateCreate') ? 'create' : 'edit';

                // draft 保存
                draftRating.set(k, {
                    mode,
                    postId: mode === 'edit' ? (postId || null) : null,
                    rating,
                    prefecture: '', // 初期
                    channelId: interaction.channelId,
                });

                // クリック元の星メッセージは片付け
                try { await interaction.message.edit({ content: '評価を受け付けました。', components: [], embeds: [] }); } catch { }

                await interaction.reply({
                    ephemeral: true,
                    content: '都道府県を選んでください（任意）',
                    components: prefPickComponents(mode, guildId, ownerId),
                });

                // reply の後、return の前で保存
                prefPromptInteraction.set(k, interaction);
                return;
            }

            // Search panel
            if (id.startsWith('search:setUser:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const row = new ActionRowBuilder().addComponents(
                    new UserSelectMenuBuilder()
                        .setCustomId(`search:userPick:${guildId}:${ownerId}`)
                        .setPlaceholder('人を選んでください（任意）')
                        .setMinValues(0)
                        .setMaxValues(1)
                );
                return interaction.reply({ ephemeral: true, content: '人を選んでね（未選択もOK）', components: [row] });
            }

            if (id.startsWith('search:setPref:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const { p, totalPages, slice } = prefSlice(0);

                const select = new StringSelectMenuBuilder()
                    .setCustomId(`search:prefPick:${guildId}:${ownerId}:${p}`)
                    .setPlaceholder(`都道府県を選んでください ${p + 1}/${totalPages}`)
                    .setMinValues(0)
                    .setMaxValues(1)
                    .addOptions(slice.map(x => ({ label: x, value: x })));

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

                return interaction.reply({
                    ephemeral: true,
                    content: '都道府県を選んでね',
                    components: [new ActionRowBuilder().addComponents(select), nav],
                });
            }

            if (id.startsWith('search:setText:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const st = searchState.get(k) ?? { userIdFilter: null, keyword: '' };
                const modal = new ModalBuilder().setCustomId(`modalSearch:${guildId}:${ownerId}`).setTitle('🔎 検索条件');

                modal.addComponents(
                    new ActionRowBuilder().addComponents(
                        new TextInputBuilder()
                            .setCustomId('keyword')
                            .setLabel('キーワード（部分一致：店名/コメント/タグ）')
                            .setStyle(TextInputStyle.Short)
                            .setRequired(false)
                            .setValue(st.keyword ?? '')
                    )
                );
                return interaction.showModal(modal);
            }

            if (id.startsWith('search:clear:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const st = { userIdFilter: null, prefectureFilter: null, keyword: '', results: [], idx: 0 };
                searchState.set(k, st);
                return interaction.update({ embeds: [searchPanelEmbed(st)], components: searchPanelComponents(guildId, userId) });
            }

            if (id.startsWith('search:back:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                return interaction.update({ embeds: [homeEmbed()], components: homeComponents() });
            }

            if (id.startsWith('search:run:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                await ensureCacheLoadedForGuild(interaction.guild);
                const cache = getGuildCache(guildId);

                const st = searchState.get(k) ?? { userIdFilter: null, keyword: '' };
                const kw = (st.keyword ?? '').trim().toLowerCase();

                const results = [...cache.values()]
                    .filter(p => {
                        if (st.userIdFilter && p.created_by !== st.userIdFilter) return false;
                        if (st.prefectureFilter) {
                            const pp = (p.prefecture ?? '').trim();
                            if (pp !== st.prefectureFilter) return false;
                        }
                        if (kw) {
                            const hay = [p.name ?? '', p.comment ?? '', p.prefecture ?? '', ...(p.tags ?? [])]
                                .join('\n')
                                .toLowerCase();
                            if (!hay.includes(kw)) return false;
                        }
                        return true;
                    })
                    .sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''));

                st.results = results.map(p => p.id);
                st.idx = 0;
                searchState.set(k, st);

                if (!st.results.length) {
                    return interaction.reply({
                        ephemeral: true,
                        content: '該当なしでした。',
                        embeds: [searchPanelEmbed(st)],
                        components: searchPanelComponents(guildId, userId),
                    });
                }

                return renderSearchResult(interaction, guildId, userId, { update: false });
            }

            // Search result nav
            if (id.startsWith('res:prev:') || id.startsWith('res:next:')) {
                const [, dir, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const st = searchState.get(k);
                if (!st?.results?.length) return interaction.reply({ ephemeral: true, content: '結果がありません。' });

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
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                await ensureCacheLoadedForGuild(interaction.guild);

                // ここから：DB正本を必ず読み直す
                const dbCh = await getDbChannelForGuild(interaction.guild);
                const dbMsg = await dbCh.messages.fetch(postId);
                const fresh = extractPostFromMessage(dbMsg);
                if (!fresh) return interaction.reply({ ephemeral: true, content: 'データが見つかりません。' });

                // ここ追加：既存のimagesを保持
                const cache = getGuildCache(guildId);
                const existed = cache.get(postId);
                fresh.images = existed?.images ?? [];

                cache.set(postId, fresh);

                const chunks = buildDetailEmbedsChunks(fresh, { sharedByUserId: userId });
                for (const embeds of chunks) {
                    await interaction.channel.send({ embeds });
                }

                return interaction.reply({ ephemeral: true, content: 'このチャンネルに送信しました。' });
            }

            // Edit from result (only if canEdit)
            if (id.startsWith('res:edit:')) {
                const [, , gid, ownerId, postId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                await ensureCacheLoadedForGuild(interaction.guild);
                const cache = getGuildCache(guildId);
                const post = cache.get(postId);
                if (!post) return interaction.reply({ ephemeral: true, content: 'データが見つかりません。' });
                if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '編集できません（投稿者のみ）。' });

                await interaction.reply({
                    ephemeral: true,
                    content: `編集する評価を選んでね（現在: ${stars(post.rating)}）`,
                    components: ratingRow('rateEdit', guildId, ownerId, postId),
                });

                // 評価メッセージ保存
                ratingPromptInteraction.set(k, interaction);
                return;
            }

            // Photos from result (only if canEdit)
            if (id.startsWith('res:photos:')) {
                const [, , gid, ownerId, postId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                await ensureCacheLoadedForGuild(interaction.guild);
                const cache = getGuildCache(guildId);
                const post = cache.get(postId);
                if (!post) return interaction.reply({ ephemeral: true, content: 'データが見つかりません。' });
                if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '写真の編集は投稿者のみです。' });

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

            // Back from result -> search panel
            if (id.startsWith('res:back:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const st = searchState.get(k) ?? { userIdFilter: null, keyword: '' };
                return interaction.update({ embeds: [searchPanelEmbed(st)], components: searchPanelComponents(guildId, userId) });
            }

            // Photo manager
            if (id.startsWith('ph:')) {
                const [, action, gid, ownerId, postId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                await ensureCacheLoadedForGuild(interaction.guild);
                const cache = getGuildCache(guildId);
                const post = cache.get(postId);
                if (!post) return interaction.reply({ ephemeral: true, content: 'データが見つかりません。' });
                if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '写真の編集は投稿者のみです。' });

                const pv = photoView.get(k) ?? { postId, idx: 0 };
                pv.postId = postId;

                const urls = imageUrls(post);
                const total = urls.length;

                if (action === 'prev' && total) pv.idx = (pv.idx - 1 + total) % total;
                if (action === 'next' && total) pv.idx = (pv.idx + 1) % total;

                if (action === 'add') {

                    await interaction.reply({
                        ephemeral: true,
                        content: '写真を送信してください（60秒以内）。送信した画像をすべて追加します。（投稿後、Botが消します）'
                    });

                    awaitingPhoto.set(k, {
                        postId,
                        channelId: interaction.channelId,
                        guildId,
                        expiresAt: Date.now() + 60_000,
                        interaction
                    });

                    return;
                }

                if (action === 'del') {
                    const urls = imageUrls(post);
                    if (!urls.length) return interaction.reply({ ephemeral: true, content: '写真がありません。' });

                    // post.images をオブジェクトとして扱う（URLだけの古い形も一応ケア）
                    const imgs = post.images ?? [];
                    const target = imgs[pv.idx];

                    // msgId が取れない（古い形式）なら「DBから消せない」ので拒否 or 表示だけ消す
                    const msgId = (typeof target === 'string') ? null : target?.msgId;
                    if (!msgId) {
                        // ここは好きに：拒否が安全
                        return interaction.reply({ ephemeral: true, content: '古い形式の写真のため、DBから削除できません（再登録が必要）。' });
                    }

                    // DBの画像メッセージを削除
                    const dbCh = await getDbChannelForGuild(interaction.guild);
                    try {
                        const imgMsg = await dbCh.messages.fetch(msgId);
                        await imgMsg.delete();
                    } catch (e) {
                        return interaction.reply({ ephemeral: true, content: 'DB側の写真メッセージ削除に失敗しました（権限/対象なし）。' });
                    }

                    // 配列からも削除
                    post.images.splice(pv.idx, 1);

                    pv.idx = Math.min(pv.idx, post.images.length - 1);
                    if (pv.idx < 0) pv.idx = 0;

                    post.updated_at = nowIso();

                    // 正本embed更新（imagesはJSONに入らないけど、更新日時などは反映）
                    const dbMsg = await dbCh.messages.fetch(postId);
                    await dbMsg.edit({ embeds: [buildPostEmbedForDb(post)] });

                    cache.set(postId, post);
                }

                if (action === 'back') {
                    const detail = buildPostEmbedForView(post);
                    detail.setTitle(`📄 詳細  ${post.name}`.trim());

                    const mine = mineState.get(k);
                    const fromMine = mine?.results?.includes(postId);

                    const components = fromMine
                        ? detailComponentsFromMine(guildId, userId, postId) // ← これに置き換え
                        : postResultComponents(guildId, userId, postId, 1, { canEditThis: canEdit(interaction, post) });

                    return interaction.update({ embeds: [detail], components });
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

            // Mine list paging/back
            if (id.startsWith('mine:prev:') || id.startsWith('mine:next:')) {
                const [, dir, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const st = mineState.get(k);
                if (!st?.results?.length) return interaction.reply({ ephemeral: true, content: '一覧がありません。' });

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
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                await interaction.deferUpdate();
                return interaction.editReply({ embeds: [homeEmbed()], components: homeComponents() });
            }

            if (id.startsWith('mine:back:')) {
                const [, , gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                return renderMineList(interaction, guildId, userId, { update: true });
            }
            return;
        }

        // User select menu (search filter user)
        if (interaction.isUserSelectMenu()) {
            const id = interaction.customId;
            if (!id.startsWith('search:userPick:')) return;

            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

            const picked = interaction.values?.[0] ?? null;
            const st = searchState.get(k) ?? { userIdFilter: null, keyword: '' };
            st.userIdFilter = picked || null;
            searchState.set(k, st);

            return interaction.reply({
                ephemeral: true,
                content: picked ? `人フィルタを <@${picked}> にしました。` : '人フィルタを解除しました。',
                embeds: [searchPanelEmbed(st)],
                components: searchPanelComponents(guildId, userId),
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
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const picked = interaction.values?.[0] ?? null; // 未選択なら null
                const st = searchState.get(k) ?? { userIdFilter: null, prefectureFilter: null, keyword: '', results: [], idx: 0 };

                st.prefectureFilter = picked || null;
                searchState.set(k, st);

                // セレクトUIを閉じて、検索パネルを更新
                return interaction.update({
                    content: picked ? `都道府県フィルタ：**${picked}**` : '都道府県フィルタを解除しました。',
                    embeds: [searchPanelEmbed(st)],
                    components: searchPanelComponents(guildId, userId),
                });
            }

            if (id.startsWith('create:prefPick:') || id.startsWith('edit:prefPick:')) {
                const parts = id.split(':');
                const mode = parts[0];     // create or edit
                const gid = parts[2];
                const ownerId = parts[3];
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const picked = interaction.values?.[0] ?? ''; // 未選択は ''
                const d = draftRating.get(k);
                if (!d || d.mode !== mode) {
                    return interaction.reply({ ephemeral: true, content: '途中状態がありません。最初からやり直して。' });
                }

                d.prefecture = picked;
                draftRating.set(k, d);

                try {
                    await interaction.message.edit({
                        content: picked ? `都道府県：**${picked}**\n入力フォームを開くよ` : '都道府県：未設定\n入力フォームを開くよ',
                        components: [],
                        embeds: [],
                    });
                } catch { }

                // 都道府県ピッカーのephemeralメッセージを消す
                const prevPref = prefPromptInteraction.get(k);
                if (prevPref) { try { await prevPref.deleteReply(); } catch { } prefPromptInteraction.delete(k); }

                if (mode === 'create') {
                    return interaction.showModal(buildCreateModal(gid, ownerId));
                } else {
                    const postId = d.postId;
                    await ensureCacheLoadedForGuild(interaction.guild);
                    const cache = getGuildCache(guildId);
                    const post = cache.get(postId);
                    if (!post) return interaction.reply({ ephemeral: true, content: '対象データが見つかりません。' });
                    if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '編集できません（投稿者のみ）。' });

                    return interaction.showModal(buildEditModal(gid, ownerId, postId, post));
                }
            }

            if (!id.startsWith('mine:pick:')) return;

            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

            const postId = interaction.values?.[0];
            if (!postId || postId === 'none') return interaction.reply({ ephemeral: true, content: '選択が不正です。' });

            await ensureCacheLoadedForGuild(interaction.guild);
            const cache = getGuildCache(guildId);
            const post = cache.get(postId);
            if (!post) return interaction.reply({ ephemeral: true, content: 'データが見つかりません。' });

            const embed = buildPostEmbedForView(post);
            embed.setTitle(`📄 詳細  ${post.name}`.trim());

            // 詳細で編集できるように（自分の記録は全部自分の投稿）
            return interaction.reply({
                ephemeral: true,
                embeds: [embed],
                components: detailComponentsFromMine(guildId, userId, postId),
            });
        }

        // Modal submit
        if (interaction.isModalSubmit()) {
            const id = interaction.customId;

            // Create
            if (id.startsWith('modalCreate:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'create') {
                    return interaction.reply({ ephemeral: true, content: '評価が未選択です。もう一度やり直してください。' });
                }

                const name = interaction.fields.getTextInputValue('name')?.trim();
                const comment = interaction.fields.getTextInputValue('comment')?.trim();
                const url = interaction.fields.getTextInputValue('url')?.trim();
                const tagsRaw = interaction.fields.getTextInputValue('tags')?.trim();
                const tags = parseTags(tagsRaw);
                const pref = (d?.prefecture ?? '').trim();

                if (!name) {
                    return interaction.reply({ ephemeral: true, content: '未入力があります（店名/コメント/URL/タグは必須）。' });
                }

                const post = {
                    id: 'TEMP',
                    name,
                    rating: d.rating,
                    comment,
                    url,
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

                // 写真追加待ち
                awaitingPhoto.set(k, {
                    postId: post.id,
                    channelId: interaction.channelId,
                    guildId,
                    expiresAt: Date.now() + 60_000,
                    interaction,
                });

                return interaction.reply({
                    ephemeral: true,
                    content:
                        `登録しました：${post.name}\n` +
                        '写真を送信してください（60秒以内）。送信した画像をすべて追加します。（投稿後、Botが消します）',
                });
            }

            // Edit
            if (id.startsWith('modalEdit:')) {
                const [, gid, ownerId, postId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                await ensureCacheLoadedForGuild(interaction.guild);
                const cache = getGuildCache(guildId);
                const post = cache.get(postId);
                if (!post) return interaction.reply({ ephemeral: true, content: '対象データが見つかりません。' });
                if (!canEdit(interaction, post)) return interaction.reply({ ephemeral: true, content: '編集できません（投稿者のみ）。' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'edit' || d.postId !== postId) {
                    return interaction.reply({ ephemeral: true, content: '評価選択が未完了です。もう一度やり直してください。' });
                }

                const name = interaction.fields.getTextInputValue('name')?.trim();
                const comment = interaction.fields.getTextInputValue('comment')?.trim();
                const url = interaction.fields.getTextInputValue('url')?.trim();
                const tagsRaw = interaction.fields.getTextInputValue('tags')?.trim();
                const tags = parseTags(tagsRaw);

                if (!name) {
                    return interaction.reply({ ephemeral: true, content: '未入力があります（店名/コメント/URL/タグは必須）。' });
                }

                post.name = name;
                post.comment = comment;
                post.url = url;
                post.tags = tags;
                post.rating = d.rating;
                const pref = (d?.prefecture ?? '').trim();
                post.prefecture = pref;
                post.updated_at = nowIso();

                const dbCh = await getDbChannelForGuild(interaction.guild);
                const dbMsg = await dbCh.messages.fetch(postId);
                await dbMsg.edit({ embeds: [buildPostEmbedForDb(post)] });

                cache.set(postId, post);

                // どこ導線か判定して、詳細を再表示
                const fromMine = cameFromMine(k, postId, mineState);
                const { detail, components } = renderDetail(interaction, { post, guildId, userId, fromMine });

                // まず画面を更新（Modalは update じゃなく reply でOK）
                await interaction.reply({ ephemeral: true, embeds: [detail], components });

                // そのあと一言（任意）
                await interaction.followUp({ ephemeral: true, content: '更新しました。' });
                return;
            }

            // Search condition
            if (id.startsWith('modalSearch:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ ephemeral: true, content: 'ギルド不一致です。' });
                if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません。' });

                const keyword = interaction.fields.getTextInputValue('keyword')?.trim() ?? '';

                const st = searchState.get(k) ?? { userIdFilter: null, keyword: '' };
                st.keyword = keyword;
                searchState.set(k, st);

                return interaction.reply({
                    ephemeral: true,
                    embeds: [searchPanelEmbed(st)],
                    components: searchPanelComponents(guildId, userId),
                });
            }
        }
    } catch (e) {
        console.error(e);
        if (interaction.isRepliable()) {
            try {
                await interaction.reply({ ephemeral: true, content: `エラー: ${e.message}` });
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

        // ephemeral消す
        if (wait.interaction) {
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
    //await registerCommands();
    //console.log('Commands registered');
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
