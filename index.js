// index.js (ESM)
// package.json に "type": "module" がある前提

import express from "express";
import dotenv from "dotenv";
import { createClient }
    from '@supabase/supabase-js';

dotenv.config();

const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';

if (!SUPABASE_URL) throw new Error('SUPABASE_URL is required');
if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error('SUPABASE_SERVICE_ROLE_KEY is required');

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: {
        persistSession: false,
        autoRefreshToken: false,
    },
});

function mapDbPostToView(row) {
    return {
        id: row.id,
        server_id: row.server_id,
        user_id: row.user_id,
        shop_id: row.shop_id,

        name: row.shop_name ?? row.shops?.name ?? '',
        prefecture: row.shop_prefecture ?? row.shops?.prefecture ?? '',
        map_url: row.shop_map_url ?? row.shops?.map_url ?? '',
        url: row.shop_website_url ?? row.shops?.website_url ?? '',

        visited: row.visited !== false,
        rating: row.rating,
        comment: row.comment ?? '',
        visited_date: row.visited_date
            ? new Date(row.visited_date).toLocaleDateString('ja-JP').replace(/\//g, '/')
            : '',

        visibility: row.visibility,
        created_at: row.created_at,
        updated_at: row.updated_at,

        created_by: row.users?.discord_user_id ?? null,
        created_by_name: row.users?.name ?? null,

        visible_server_row_ids: (row.post_visible_servers ?? [])
            .map(x => x.server_id)
            .filter(Boolean),

        visible_server_ids: (row.post_visible_servers ?? [])
            .map(x => x.servers?.discord_server_id)
            .filter(Boolean),

        tags: (row.post_tags ?? [])
            .map(x => x.tags?.name)
            .filter(Boolean),

        images: (row.post_images ?? [])
            .sort((a, b) => (a.sort_order ?? 0) - (b.sort_order ?? 0))
            .map(x => ({
                id: x.id,
                url: x.image_url,
                storage_path: x.storage_path ?? null,
                sort_order: x.sort_order ?? 0,
            })),
    };
}

const app = express();
const PORT = Number(process.env.PORT || 5000);
app.get('/', (_req, res) => {
    return res.status(200).send('ok');
});

app.get('/healthcheck', (_req, res) => {
    return res.status(200).send('ok');
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
    MessageFlags,
}
    from 'discord.js';

console.log('ENV CHECK', {
    tokenLen: (process.env.DISCORD_TOKEN || '').length,
    dbName: process.env.DB_CHANNEL_NAME,
});

const TOKEN = process.env.DISCORD_TOKEN;
const DB_CHANNEL_NAME = (process.env.DB_CHANNEL_NAME || 'gourmet-db').toLowerCase();
const GOOGLE_MAPS_API_KEY = process.env.GOOGLE_MAPS_API_KEY || '';

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

//// ====== 保存方式（DBなし：保存用チャンネルのEmbedが正本） ======
//const DATA_MARK = '__DATA__:';
// ====== 保存方式（Supabase正本 / guildごとにキャッシュ保持） ======

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

// 写真削除確認の一時状態 key: guildId:userId -> { postId, idx }
const deletePhotoConfirmState = new Map();

// 全写真削除確認の一時状態 key: guildId:userId -> { postId }
const deleteAllPhotosConfirmState = new Map();

// 詳細画面の戻り先状態 key: guildId:userId -> { postId, fromMine, forceHomeBack }
const detailNavState = new Map();

// 作成後に詳細を開くか確認する一時状態 key: guildId:userId -> { postId }
const openDetailAfterCreateState = new Map();

// 検索キーワードモーダルを閉じたあと元の検索パネルを更新するため
// key: guildId:userId -> { webhook, messageId }
const searchKeywordPromptRef = new Map();

// 新規登録パネル参照
// key: guildId:userId -> { webhook, messageId }
const createPanelPromptRef = new Map();

// 編集パネル参照
// key: guildId:userId -> { webhook, messageId }
const editPanelPromptRef = new Map();

// お店検索状態 key: guildId:userId
// {
//   mode: 'create'|'edit',
//   query: string,
//   results: [{ placeId, name, address, mapUrl }],
//   page: number,
//   nextPageToken: string,
//   loadingMore: boolean,
// }
const placeSearchState = new Map();

const CONFIRM_KIND = {
    DELETE_POST: 'deletePost',
    DELETE_PHOTO: 'deletePhoto',
    DELETE_ALL_PHOTOS: 'deleteAllPhotos',
    OPEN_DETAIL_AFTER_CREATE: 'openDetailAfterCreate',
};

// ====== util ======
async function getPostByIdForViewer(postId, guildId, viewerDiscordUserId) {
    if (!postId) return null;

    const serverRow = await ensureServerRowByGuildId(guildId);

    const fresh = await refreshPostCacheById(postId, guildId);
    if (fresh && canViewPost({
        viewerDiscordUserId,
        viewerDiscordGuildId: guildId,
        viewerServerRowId: serverRow?.id ?? null,
        post: fresh,
    })) {
        return fresh;
    }

    const cache = getGuildCache(guildId);
    const cached = cache.get(postId);
    if (cached && canViewPost({
        viewerDiscordUserId,
        viewerDiscordGuildId: guildId,
        viewerServerRowId: serverRow?.id ?? null,
        post: cached,
    })) {
        return cached;
    }

    const privatePosts = await getPrivatePostsForViewer(guildId, viewerDiscordUserId);
    const mine = privatePosts.find(x => x.id === postId) ?? null;

    if (mine && canViewPost({
        viewerDiscordUserId,
        viewerDiscordGuildId: guildId,
        viewerServerRowId: serverRow?.id ?? null,
        post: mine,
    })) {
        return mine;
    }

    return null;
}

function nowIso() {
    return new Date().toISOString();
}

async function deleteImageFileFromStorage(storagePath) {
    if (!storagePath) return;

    const normalized = String(storagePath).replace(/^\/+/, '')

    const { error } = await supabase.storage
        .from('post-images')
        .remove([normalized]);

    if (error) throw error;
}

async function deletePostImageWithStorage(imageRow) {
    if (!imageRow?.id) {
        throw new Error('image id is required');
    }

    if (imageRow.storage_path) {
        await deleteImageFileFromStorage(imageRow.storage_path);
    }

    const { error } = await supabase
        .from('post_images')
        .delete()
        .eq('id', imageRow.id);

    if (error) throw error;
}

async function deleteAllPostImagesWithStorage(postId) {
    const { data, error } = await supabase
        .from('post_images')
        .select('id, storage_path')
        .eq('post_id', postId);

    if (error) throw error;

    const paths = (data ?? [])
        .map(x => x.storage_path)
        .filter(Boolean)
        .map(x => String(x).replace(/^\/+/, ''));

    if (paths.length) {
        const { error: storageError } = await supabase.storage
            .from('post-images')
            .remove(paths);

        if (storageError) throw storageError;
    }

    const { error: deleteError } = await supabase
        .from('post_images')
        .delete()
        .eq('post_id', postId);

    if (deleteError) throw deleteError;
}

async function deletePostWithImagesAndStorage(postId) {
    const { data: images, error: imageErr } = await supabase
        .from('post_images')
        .select('id, storage_path')
        .eq('post_id', postId);

    if (imageErr) throw imageErr;

    const paths = (images ?? [])
        .map(x => x.storage_path)
        .filter(Boolean)
        .map(x => String(x).replace(/^\/+/, ''));

    if (paths.length) {
        const { error: storageError } = await supabase.storage
            .from('post-images')
            .remove(paths);

        if (storageError) throw storageError;
    }

    const { error: deleteImagesErr } = await supabase
        .from('post_images')
        .delete()
        .eq('post_id', postId);

    if (deleteImagesErr) throw deleteImagesErr;

    const { error: deletePostErr } = await supabase
        .from('posts')
        .delete()
        .eq('id', postId);

    if (deletePostErr) throw deletePostErr;
}

async function deletePostDb(postId) {
    const { error: imgErr } = await supabase
        .from('post_images')
        .delete()
        .eq('post_id', postId);
    if (imgErr) throw imgErr;

    const { error: tagErr } = await supabase
        .from('post_tags')
        .delete()
        .eq('post_id', postId);
    if (tagErr) throw tagErr;

    const { error: postErr } = await supabase
        .from('posts')
        .delete()
        .eq('id', postId);
    if (postErr) throw postErr;
}

async function updatePostInDb(guild, discordUserId, d) {
    const serverRow = await ensureServerRow(guild);
    const userRow = await ensureUserRow(discordUserId, guild);
    const shopRow = await upsertShopFromDraft(d);

    const payload = {
        server_id: serverRow.id,
        user_id: userRow.id,
        shop_id: shopRow?.id ?? null,

        shop_name: d.name?.trim() || '(名称不明)',
        shop_prefecture: d.prefecture?.trim() || null,
        shop_map_url: d.mapUrl?.trim() || null,
        shop_website_url: d.url?.trim() || null,

        visited: d.visited !== false,
        rating: d.visited === false ? null : Number(d.rating),
        comment: d.comment?.trim() || null,
        visited_date: d.visited === false ? null : normalizeDateForDb(d.visitedDate),
        visibility: d.visibility || 'server',
    };

    const { error } = await supabase
        .from('posts')
        .update(payload)
        .eq('id', d.postId);

    if (error) throw error;

    await replacePostTagsDb(d.postId, d.tags ?? []);
    await replacePostVisibleServers(
        d.postId,
        (d.visibility || 'server') === 'server'
            ? (d.visibleServerIds?.length ? d.visibleServerIds : [guild.id])
            : []
    );

    const { data, error: reloadErr } = await supabase
        .from('posts')
        .select(`
            id,
            server_id,
            user_id,
            shop_id,
            shop_name,
            shop_prefecture,
            shop_map_url,
            shop_website_url,
            visited,
            rating,
            comment,
            visited_date,
            visibility,
            created_at,
            updated_at,
            users!posts_user_id_fkey (
                id,
                discord_user_id,
                name
            ),
            post_images (
                id,
                image_url,
                storage_path,
                sort_order
            ),
            post_tags (
                tag_id,
                tags (
                    id,
                    name
                )
            ),
            post_visible_servers (
                server_id,
                servers (
                    id,
                    discord_server_id,
                    name
                )
            )
        `)
        .eq('id', d.postId)
        .single();

    if (reloadErr) throw reloadErr;

    return data;
}

async function createPostInDb(guild, discordUserId, d) {
    const serverRow = await ensureServerRow(guild);
    const userRow = await ensureUserRow(discordUserId, guild);
    const shopRow = await upsertShopFromDraft(d);

    const payload = {
        server_id: serverRow.id,
        user_id: userRow.id,
        shop_id: shopRow?.id ?? null,

        shop_name: d.name?.trim() || '(名称不明)',
        shop_prefecture: d.prefecture?.trim() || null,
        shop_map_url: d.mapUrl?.trim() || null,
        shop_website_url: d.url?.trim() || null,

        visited: d.visited !== false,
        rating: d.visited === false ? null : Number(d.rating),
        comment: d.comment?.trim() || null,
        visited_date: d.visited === false
            ? null
            : normalizeDateForDb(d.visitedDate),

        visibility: d.visibility || 'server',
    };

    const { data: inserted, error } = await supabase
        .from('posts')
        .insert(payload)
        .select('id')
        .single();

    if (error) throw error;

    await replacePostTagsDb(inserted.id, d.tags ?? []);
    await replacePostVisibleServers(
        inserted.id,
        (d.visibility || 'server') === 'server'
            ? (d.visibleServerIds?.length ? d.visibleServerIds : [guild.id])
            : []
    );

    const { data, error: reloadErr } = await supabase
        .from('posts')
        .select(`
            id,
            server_id,
            user_id,
            shop_id,
            shop_name,
            shop_prefecture,
            shop_map_url,
            shop_website_url,
            visited,
            rating,
            comment,
            visited_date,
            visibility,
            created_at,
            updated_at,
            users!posts_user_id_fkey (
                id,
                discord_user_id,
                name
            ),
            post_images (
                id,
                image_url,
                storage_path,
                sort_order
            ),
            post_tags (
                tag_id,
                tags (
                    id,
                    name
                )
            ),
            post_visible_servers (
                server_id,
                servers (
                    id,
                    discord_server_id,
                    name
                )
            )
        `)
        .eq('id', inserted.id)
        .single();

    if (reloadErr) throw reloadErr;

    return data;
}

async function replacePostVisibleServers(postId, discordGuildIds = []) {
    const guildIds = [...new Set((discordGuildIds ?? []).map(x => String(x).trim()).filter(Boolean))];

    const { error: delErr } = await supabase
        .from('post_visible_servers')
        .delete()
        .eq('post_id', postId);
    if (delErr) throw delErr;

    if (!guildIds.length) return;

    const { data: serverRows, error: srvErr } = await supabase
        .from('servers')
        .select('id, discord_server_id')
        .in('discord_server_id', guildIds);

    if (srvErr) throw srvErr;

    const rows = (serverRows ?? []).map(x => ({
        post_id: postId,
        server_id: x.id,
    }));

    if (!rows.length) return;

    const { error: insErr } = await supabase
        .from('post_visible_servers')
        .insert(rows);

    if (insErr) throw insErr;
}

async function replacePostTagsDb(postId, tags = []) {
    const normalized = uniqueStrings(tags);

    const { error: delErr } = await supabase
        .from('post_tags')
        .delete()
        .eq('post_id', postId);
    if (delErr) throw delErr;

    if (!normalized.length) return;

    const tagMap = await ensureTagRows(normalized);

    const rows = normalized
        .map(name => {
            const tagId = tagMap.get(name);
            if (!tagId) return null;
            return { post_id: postId, tag_id: tagId };
        })
        .filter(Boolean);

    const { error: insErr } = await supabase
        .from('post_tags')
        .insert(rows);

    if (insErr) throw insErr;
}

async function addPostImagesDb(guildId, postId, files = []) {
    const rows = [];

    const { data: existing, error: existingErr } = await supabase
        .from('post_images')
        .select('sort_order')
        .eq('post_id', postId)
        .order('sort_order', { ascending: false })
        .limit(1);

    if (existingErr) throw existingErr;

    let nextSort = (existing?.[0]?.sort_order ?? -1) + 1;

    for (const file of files) {
        const buffer = await fetchImageAsBuffer(file.url);

        const uploaded = await uploadPostImageToStorage({
            guildId,
            postId,
            sourceBuffer: buffer,
            filename: file.name || 'image',
            contentType: file.contentType || 'application/octet-stream',
        });

        rows.push({
            post_id: postId,
            image_url: uploaded.url,
            storage_path: uploaded.path,
            sort_order: nextSort++,
        });
    }

    if (!rows.length) return;

    const { error } = await supabase
        .from('post_images')
        .insert(rows);

    if (error) throw error;
}

async function uploadPostImageToStorage({
    guildId,
    postId,
    sourceBuffer,
    filename,
    contentType,
}) {

    // ファイル名を安全化
    const safeName = (filename || 'image')
        .replace(/[^\w.-]/g, '_')
        .slice(0, 120);

    // 保存パス
    const path = `${guildId}/${postId}/${Date.now()}-${safeName}`;

    const { error: uploadError } = await supabase.storage
        .from('post-images')
        .upload(path, sourceBuffer, {
            contentType: contentType || 'application/octet-stream',
            upsert: false,
        });

    if (uploadError) {
        throw new Error(`画像アップロード失敗: ${uploadError.message}`);
    }

    // 公開URL取得
    const { data } = supabase.storage
        .from('post-images')
        .getPublicUrl(path);

    return {
        path,
        url: data.publicUrl,
    };
}

async function fetchImageAsBuffer(url) {
    const res = await fetch(url);
    if (!res.ok) {
        throw new Error(`画像取得失敗: ${res.status} ${url}`);
    }

    const arrayBuffer = await res.arrayBuffer();
    return Buffer.from(arrayBuffer);
}

function normalizeDateForDb(s) {
    const v = normalizeVisitedDate(s);
    if (!v) return null;
    return v.replace(/\//g, '-');
}

function uniqueStrings(arr = []) {
    return [...new Set(
        arr
            .map(x => String(x ?? '').trim())
            .filter(Boolean)
    )];
}

function visibilityToDb(_post) {
    return 'server';
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
    }
    catch (e) {
        console.error('blankCurrentMessage failed:', e);
    }
}

async function blankMessageById(interaction, messageId) {
    try {
        if (!messageId) return;
        await interaction.webhook.deleteMessage(messageId);
    }
    catch (e) {
        console.error('blankMessageById failed:', e);
    }
}

async function blankPromptRef(ref) {
    try {
        if (!ref?.webhook || !ref?.messageId) return;
        await ref.webhook.deleteMessage(ref.messageId);
    }
    catch (e) {
        console.error('blankPromptRef failed:', e);
    }
}

async function editPromptRef(ref, payload) {
    try {
        if (!ref?.webhook || !ref?.messageId) return false;
        await ref.webhook.editMessage(ref.messageId, payload);
        return true;
    }
    catch (e) {
        console.error('editPromptRef failed:', e);
        return false;
    }
}

async function deletePromptRef(ref) {
    try {
        if (!ref?.webhook || !ref?.messageId) return true;
        await ref.webhook.deleteMessage(ref.messageId);
        return true;
    }
    catch (e) {
        console.error('deletePromptRef failed:', e);
        return false;
    }
}

async function clearEphemeralMessage(interaction) {
    try {
        if (interaction?.message?.id) {
            await interaction.webhook.deleteMessage(interaction.message.id);
        }
    }
    catch { }
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
    }
    catch {
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
        }
        catch { }
    }

    if (keepMessageId) {
        uiMessages.set(k, new Set([keepMessageId]));
    }
    else {
        uiMessages.delete(k);
    }
}

function imageUrls(post) {
    const imgs = post.images ?? [];
    return imgs
        .map(x => (typeof x === 'string' ? x : x?.url))
        .filter(x => typeof x === 'string' && x.trim().length > 0)
        .map(x => x.trim());
}


function buildDetailEmbedsChunks(post, { sharedByUserId = null } = {}) {
    const top = [];

    top.push(visitLabel(post));

    if (post.visited !== false) {
        top.push(hasRating(post) ? stars(post.rating) : '評価なし');
    }

    if (post.comment) {
        top.push('');
        top.push(safeText(post.comment, 1500));
    }

    const body = [
        ...top,
        '',
        `🗾 ${safeText(post.prefecture || '(未設定)', 100)}`,
        ...(post.visited !== false && post.visited_date ? [`📅 ${safeText(post.visited_date, 20)}`] : []),
        `🏷 ${safeText(tagString(post.tags), 500)}`,
        `👤 登録者 <@${post.created_by}>`,
        ...(sharedByUserId ? [`📤 共有 <@${sharedByUserId}>`] : []),
    ].join('\n');

    const info = new EmbedBuilder()
        .setTitle(`🍽 ${safeText(post.name || '(名称不明)', 200)}`)
        .setDescription(safeText(body, 4000))
        .addFields(
            { name: '🔗 Webサイト', value: safeText(post.url || '(なし)', 1000) },
            { name: '📍 場所', value: safeText(post.map_url || '(なし)', 1000) }
        );

    if (post.updated_at) {
        info.setFooter({ text: safeText(`更新: ${formatJst(post.updated_at)}`, 200) });
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

function visibilityLabel(v) {
    if (v === 'private') return '🔒 非公開';
    if (v === 'public') return '🌍 全体公開';
    return '🖥 サーバー公開';
}

function canViewPost({ viewerDiscordUserId, viewerDiscordGuildId, viewerServerRowId, post }) {
    if (!post) return false;

    if (post.visibility === 'private') {
        return post.created_by === viewerDiscordUserId;
    }

    if (post.visibility === 'public') {
        return true;
    }

    if (post.visibility === 'server') {
        const rowIds = post.visible_server_row_ids ?? [];
        const guildIds = post.visible_server_ids ?? [];

        if (viewerServerRowId && rowIds.includes(viewerServerRowId)) return true;
        if (viewerDiscordGuildId && guildIds.includes(viewerDiscordGuildId)) return true;

        return false;
    }

    return false;
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

    // YYYY-MM-DD / YYYY/MM/DD を許可
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

function safeText(v, max = 1000) {
    return String(v ?? '').slice(0, max);
}

function buildPostEmbedForView(post, { sharedByUserId = null, imageIndex = null, notice = null } = {}) {
    const lines = [];

    if (notice) {
        lines.push(`📤 ${safeText(notice, 200)}`);
        lines.push('');
    }

    lines.push(visitLabel(post));

    if (hasRating(post)) {
        lines.push(stars(post.rating));
        lines.push('');
    }

    if (post.comment) {
        lines.push(safeText(post.comment, 1500));
        lines.push('');
    }

    lines.push(`🗾 ${safeText(post.prefecture || '(未設定)', 100)}`);

    if (post.visited !== false && post.visited_date) {
        lines.push(`📅 ${safeText(post.visited_date, 20)}`);
    }

    lines.push(`🏷 ${safeText(tagString(post.tags), 500)}`);
    lines.push(`🌐 ${visibilityLabel(post.visibility)}`);
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
        { name: '🔗 Webサイト', value: safeText(post.url || '(なし)', 1000) },
        { name: '📍 場所', value: safeText(post.map_url || '(なし)', 1000) },
    ];

    const e = new EmbedBuilder()
        .setTitle(`🍽 ${safeText(post.name || '(名称不明)', 200)}`)
        .setDescription(safeText(lines.join('\n'), 4000))
        .addFields(fields);

    if (post.updated_at) {
        e.setFooter({ text: safeText(`更新: ${formatJst(post.updated_at)}`, 200) });
    }

    if (urls.length) {
        const idx = Math.max(0, Math.min(urls.length - 1, Number(imageIndex) || 0));
        e.setImage(urls[idx]);
    }

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

async function mergeViewerPrivatePostsIntoCache(guildId, discordUserId) {
    if (!discordUserId) return;

    const privatePosts = await getPrivatePostsForViewer(guildId, discordUserId);
    const cache = getGuildCache(guildId);

    for (const post of privatePosts) {
        cache.set(post.id, post);
    }
}

async function ensureCacheLoadedForGuild(guild, discordUserId = null) {
    const guildId = guild.id;

    if (cacheReadyByGuild.get(guildId)) {
        await mergeViewerPrivatePostsIntoCache(guildId, discordUserId);
        return;
    }

    const { data: serverRow, error: serverErr } = await supabase
        .from('servers')
        .select('id, discord_server_id')
        .eq('discord_server_id', guildId)
        .maybeSingle();

    if (serverErr) throw serverErr;

    const cache = getGuildCache(guildId);
    cache.clear();

    if (!serverRow) {
        cacheReadyByGuild.set(guildId, true);
        return;
    }

    const { data, error } = await supabase
        .from('posts')
        .select(`
            id,
            server_id,
            user_id,
            shop_id,
            shop_name,
            shop_prefecture,
            shop_map_url,
            shop_website_url,
            visited,
            rating,
            comment,
            visited_date,
            visibility,
            created_at,
            updated_at,
            users!posts_user_id_fkey (
                id,
                discord_user_id,
                name
            ),
            post_images (
                id,
                image_url,
                storage_path,
                sort_order
            ),
            post_tags (
                tag_id,
                tags (
                    id,
                    name
                )
            ),
            post_visible_servers (
                server_id,
                servers (
                    id,
                    discord_server_id,
                    name
                )
            )
        `)
        .in('visibility', ['public', 'server'])
        .order('created_at', { ascending: false });

    if (error) throw error;

    for (const row of data ?? []) {
        const post = mapDbPostToView(row);

        if (!canViewPost({
            viewerDiscordUserId: discordUserId,
            viewerDiscordGuildId: guildId,
            viewerServerRowId: serverRow.id,
            post,
        })) {
            continue;
        }

        cache.set(post.id, post);
    }

    await mergeViewerPrivatePostsIntoCache(guildId, discordUserId);

    cacheReadyByGuild.set(guildId, true);
}

async function ensureServerRowByGuildId(guildId) {
    const { data, error } = await supabase
        .from('servers')
        .select('id, discord_server_id')
        .eq('discord_server_id', guildId)
        .maybeSingle();

    if (error) throw error;
    return data;
}


async function getPrivatePostsForViewer(guildId, viewerDiscordUserId) {
    const { data: userRow, error: userErr } = await supabase
        .from('users')
        .select('id, discord_user_id')
        .eq('discord_user_id', viewerDiscordUserId)
        .maybeSingle();

    if (userErr) throw userErr;
    if (!userRow) return [];

    const { data, error } = await supabase
        .from('posts')
        .select(`
            id,
            server_id,
            user_id,
            shop_id,
            shop_name,
            shop_prefecture,
            shop_map_url,
            shop_website_url,
            visited,
            rating,
            comment,
            visited_date,
            visibility,
            created_at,
            updated_at,
            users!posts_user_id_fkey (
                id,
                discord_user_id,
                name
            ),
            post_images (
                id,
                image_url,
                storage_path,
                sort_order
            ),
            post_tags (
                tag_id,
                tags (
                    id,
                    name
                )
            ),
            post_visible_servers (
                server_id,
                servers (
                    id,
                    discord_server_id,
                    name
                )
            )
        `)
        .eq('user_id', userRow.id)
        .eq('visibility', 'private')
        .order('created_at', { ascending: false });

    if (error) throw error;

    return (data ?? []).map(mapDbPostToView);
}

async function refreshPostCacheById(postId, guildId) {
    if (!postId) return null;

    const cache = getGuildCache(guildId);
    const serverRow = await ensureServerRowByGuildId(guildId);

    const { data, error } = await supabase
        .from('posts')
        .select(`
            id,
            server_id,
            user_id,
            shop_id,
            shop_name,
            shop_prefecture,
            shop_map_url,
            shop_website_url,
            visited,
            rating,
            comment,
            visited_date,
            visibility,
            created_at,
            updated_at,
            users!posts_user_id_fkey (
                id,
                discord_user_id,
                name
            ),
            post_images (
                id,
                image_url,
                storage_path,
                sort_order
            ),
            post_tags (
                tag_id,
                tags (
                    id,
                    name
                )
            ),
            post_visible_servers (
                server_id,
                servers (
                    id,
                    discord_server_id,
                    name
                )
            )
        `)
        .eq('id', postId)
        .maybeSingle();

    if (error) throw error;

    if (!data) {
        cache.delete(postId);
        return null;
    }

    const post = mapDbPostToView(data);

    if (post.visibility === 'private') {
        cache.delete(postId);
        return post;
    }

    if (!canViewPost({
        viewerDiscordUserId: null,
        viewerDiscordGuildId: guildId,
        viewerServerRowId: serverRow?.id ?? null,
        post,
    })) {
        cache.delete(postId);
        return post;
    }

    cache.set(post.id, post);
    return post;
}

async function upsertShopFromDraft(d) {
    const place = d.place ?? null;

    if (!place?.placeId) {
        return null;
    }

    const payload = {
        place_id: place.placeId,
        name: place.name || d.name?.trim() || '(名称不明)',
        address: place.address || null,
        prefecture: d.prefecture || null,
        map_url: place.mapUrl || d.mapUrl || null,
        website_url: d.url || null,
    };

    const { data, error } = await supabase
        .from('shops')
        .upsert(payload, { onConflict: 'place_id' })
        .select('*')
        .single();

    if (error) throw error;
    return data;
}

function canEdit(interaction, post) {
    if (!post) return false;

    return post.created_by === interaction.user.id;
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

const PLACE_PAGE_SIZE = 25;

function placeSlice(results = [], page = 0) {
    const totalPages = Math.max(1, Math.ceil((results?.length || 0) / PLACE_PAGE_SIZE));
    const p = Math.max(0, Math.min(totalPages - 1, Number(page) || 0));
    const start = p * PLACE_PAGE_SIZE;

    return {
        p,
        totalPages,
        slice: (results ?? []).slice(start, start + PLACE_PAGE_SIZE),
    };
}

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function searchGooglePlacesText(query, pageToken = '') {
    if (!GOOGLE_MAPS_API_KEY) {
        throw new Error('GOOGLE_MAPS_API_KEY が設定されていません');
    }

    const body = {
        textQuery: query,
        languageCode: 'ja',
        regionCode: 'JP',
        ...(pageToken ? { pageToken } : {}),
    };

    const res = await fetch('https://places.googleapis.com/v1/places:searchText', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': GOOGLE_MAPS_API_KEY,
            'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.googleMapsUri,nextPageToken',
        },
        body: JSON.stringify(body),
    });

    if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`Google Places検索に失敗しました: ${res.status} ${text}`);
    }

    const json = await res.json();

    return {
        results: (json.places ?? []).map(p => ({
            placeId: p.id ?? '',
            name: p.displayName?.text ?? '',
            address: p.formattedAddress ?? '',
            mapUrl: p.googleMapsUri ?? '',
        })).filter(x => x.name && x.mapUrl),
        nextPageToken: json.nextPageToken ?? '',
    };
}

async function fetchMoreGooglePlaces(query, nextPageToken) {
    if (!nextPageToken) {
        return { results: [], nextPageToken: '' };
    }

    // nextPageToken は少し待たないと無効なことがある
    await sleep(2000);
    return searchGooglePlacesText(query, nextPageToken);
}

function buildPlaceSearchModal(gid, ownerId, mode, currentValue = '') {
    return new ModalBuilder()
        .setCustomId(`modalPlaceSearch:${gid}:${ownerId}:${mode}`)
        .setTitle('🍽 お店を検索')
        .addComponents(
            new ActionRowBuilder().addComponents(
                new TextInputBuilder()
                    .setCustomId('placeQuery')
                    .setLabel('店名・地域など')
                    .setStyle(TextInputStyle.Short)
                    .setRequired(true)
                    .setPlaceholder('例: 一蘭 渋谷 / スシロー 浜松')
                    .setValue(currentValue ?? '')
            )
        );
}

function placeSearchComponents(guildId, userId, st) {
    const { p, totalPages, slice } = placeSlice(st.results ?? [], st.page ?? 0);

    const options = slice?.length
        ? slice.slice(0, 25).map((x, i) => ({
            label: String(x?.name ?? '(名称不明)').slice(0, 100),
            description: String(x?.address ?? '住所なし').slice(0, 100),
            value: String((p * PLACE_PAGE_SIZE) + i).slice(0, 100),
        }))
        : [{ label: '(候補なし)', description: '選択できません', value: 'none' }];

    return [
        new ActionRowBuilder().addComponents(
            new StringSelectMenuBuilder()
                .setCustomId(`place:pick:${guildId}:${userId}`)
                .setPlaceholder(`候補を選択してください ${p + 1}/${totalPages}`)
                .setMinValues(1)
                .setMaxValues(1)
                .addOptions(options)
        ),
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`place:prev:${guildId}:${userId}`)
                .setLabel('◀ 前へ')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(p <= 0),

            new ButtonBuilder()
                .setCustomId(`place:next:${guildId}:${userId}`)
                .setLabel('次へ ▶')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(p >= totalPages - 1),

            new ButtonBuilder()
                .setCustomId(`place:more:${guildId}:${userId}`)
                .setLabel('さらに読み込む')
                .setStyle(ButtonStyle.Secondary)
                .setDisabled(!st?.nextPageToken || st?.loadingMore),

            new ButtonBuilder()
                .setCustomId(`place:back:${guildId}:${userId}`)
                .setLabel('戻る')
                .setStyle(ButtonStyle.Secondary),
        ),
    ];
}

function buildPlaceSearchEmbed(st = {}) {
    const total = st?.results?.length || 0;
    const query = st?.query || '';

    return new EmbedBuilder()
        .setTitle('🍽 お店検索結果')
        .setDescription(
            `検索語: ${query ? `「${query}」` : '(なし)'}\n` +
            `取得件数: ${total}件\n\n` +
            '候補を選ぶと、お店の名前とGoogleMapリンクを自動入力します'
        );
}

async function renderPlaceSearchPicker(interaction, guildId, userId, { update = true } = {}) {
    const k = keyOf(guildId, userId);
    const st = placeSearchState.get(k);

    if (!st) {
        return interaction.reply({
            flags: MessageFlags.Ephemeral,
            content: 'お店検索状態がありません',
        });
    }

    const payload = {
        content: '',
        embeds: [buildPlaceSearchEmbed(st)],
        components: placeSearchComponents(guildId, userId, st),
    };

    if (update) {
        return interaction.update(payload);
    }

    return interaction.reply({
        flags: MessageFlags.Ephemeral,
        ...payload,
    });
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
        `✅ 訪問状態: ${d.visited == null ? '(未選択)' : (d.visited ? '行った' : '行きたい')}`,
        ...(d.visited === true
            ? [`⭐ 評価: ${d.rating ? stars(d.rating) : '(未選択)'}`]
            : []),
        `💬 コメント: ${d.comment?.trim() ? d.comment : '(なし)'}`,
        `🗾 都道府県: ${d.prefecture || '(未設定)'}`,
        ...(d.visited === true
            ? [`📅 行った日付: ${d.visitedDate || '(未入力)'}`]
            : []),
        `🏷 タグ: ${d.tags?.length ? tagString(d.tags) : '(なし)'}`,
        `🔗 Webサイト: ${d.url || '(なし)'}`,
        `📍 場所: ${d.mapUrl || '(なし)'}`,
        `📷 写真: 記録作成後に追加`,
        '',
        '項目を入力して「作成」を押してください',
    ];

    return new EmbedBuilder()
        .setTitle('➕ 記録作成中')
        .setDescription(lines.join('\n'));
}

function buildEditPanelEmbed(d = {}) {
    const lines = [
        `🍽 お店の名前: ${d.name?.trim() ? d.name : '(未入力)'}`,
        `✅ 訪問状態: ${d.visited == null ? '(未選択)' : (d.visited ? '行った' : '行きたい')}`,
        ...(d.visited === true
            ? [`⭐ 評価: ${d.rating ? stars(d.rating) : '(未選択)'}`]
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
        '項目を編集して「更新」を押してください',
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
            .setCustomId(`create:searchPlace:${guildId}:${userId}`)
            .setLabel('お店を検索')
            .setStyle(ButtonStyle.Primary),

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

    const row2 = [
        new ButtonBuilder()
            .setCustomId(`create:setPref:${guildId}:${userId}`)
            .setLabel('都道府県')
            .setStyle(ButtonStyle.Secondary),

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
    ];

    return [
        new ActionRowBuilder().addComponents(...row1),

        new ActionRowBuilder().addComponents(...row2),

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
            .setCustomId(`edit:searchPlace:${guildId}:${userId}`)
            .setLabel('お店を検索')
            .setStyle(ButtonStyle.Primary),

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

    const row2 = [
        new ButtonBuilder()
            .setCustomId(`edit:setPref:${guildId}:${userId}`)
            .setLabel('都道府県')
            .setStyle(ButtonStyle.Secondary),

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
    ];

    return [
        new ActionRowBuilder().addComponents(...row1),

        new ActionRowBuilder().addComponents(...row2),

        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`edit:photos:${guildId}:${userId}`)
                .setLabel('写真管理')
                .setStyle(ButtonStyle.Secondary),

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

async function getServerSettingsByGuildId(guildId) {
    const serverRow = await ensureServerRowByGuildId(guildId);
    if (!serverRow) {
        return { default_visibility: 'server', allow_public_post: true };
    }

    const { data, error } = await supabase
        .from('server_settings')
        .select('default_visibility, allow_public_post')
        .eq('server_id', serverRow.id)
        .maybeSingle();

    if (error) throw error;

    return data ?? { default_visibility: 'server', allow_public_post: true };
}

async function renderCreatePanel(interaction, guildId, userId, { update = true } = {}) {
    const k = keyOf(guildId, userId);
    const settings = await getServerSettingsByGuildId(guildId);

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
        visibility: settings.default_visibility || 'server',
        visibleServerIds: [guildId],
    };

    if (!d.visibility) {
        d.visibility = settings.default_visibility || 'server';
    }

    if (!Array.isArray(d.visibleServerIds) || !d.visibleServerIds.length) {
        d.visibleServerIds = [guildId];
    }

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
        flags: MessageFlags.Ephemeral,
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
            flags: MessageFlags.Ephemeral,
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
        flags: MessageFlags.Ephemeral,
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
            flags: MessageFlags.Ephemeral,
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
                    flags: MessageFlags.Ephemeral,
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
            flags: MessageFlags.Ephemeral,
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
                    flags: MessageFlags.Ephemeral,
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

function deletePhotoConfirmComponents(guildId, userId) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`confirm:yes:dp:${guildId}:${userId}`)
                .setLabel('はい')
                .setStyle(ButtonStyle.Danger),

            new ButtonBuilder()
                .setCustomId(`confirm:no:dp:${guildId}:${userId}`)
                .setLabel('いいえ')
                .setStyle(ButtonStyle.Secondary)
        )
    ];
}

function deleteAllPhotosConfirmComponents(guildId, userId) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`confirm:yes:da:${guildId}:${userId}`)
                .setLabel('はい')
                .setStyle(ButtonStyle.Danger),

            new ButtonBuilder()
                .setCustomId(`confirm:no:da:${guildId}:${userId}`)
                .setLabel('いいえ')
                .setStyle(ButtonStyle.Secondary)
        )
    ];
}

function openDetailAfterCreateComponents(guildId, userId) {
    return [
        new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`confirm:yes:od:${guildId}:${userId}`)
                .setLabel('はい')
                .setStyle(ButtonStyle.Primary),

            new ButtonBuilder()
                .setCustomId(`confirm:no:od:${guildId}:${userId}`)
                .setLabel('いいえ')
                .setStyle(ButtonStyle.Secondary)
        )
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
            new ButtonBuilder().setCustomId('home:mine').setLabel('📚 自分の記録').setStyle(ButtonStyle.Secondary),
            new ButtonBuilder().setCustomId('home:close').setLabel('❌ 終了').setStyle(ButtonStyle.Danger)
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

    e.setTitle(`⚠ お店情報を削除: ${post.name}`);

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

// ====== Supabase migration ======
async function ensureServerRow(guild) {
    const payload = {
        discord_server_id: guild.id,
        name: guild.name ?? 'unknown',
        icon_url: guild.iconURL?.() ?? null,
    };

    const { data, error } = await supabase
        .from('servers')
        .upsert(payload, { onConflict: 'discord_server_id' })
        .select('id, discord_server_id')
        .single();

    if (error) throw error;
    return data;
}

async function ensureUserRow(discordUserId, guild) {
    let member = null;
    try {
        member = await guild.members.fetch(discordUserId);
    } catch { }

    const payload = {
        discord_user_id: discordUserId,
        name: member?.user?.username ?? null,
        avatar_url: member?.user?.displayAvatarURL?.() ?? null,
    };

    const { data, error } = await supabase
        .from('users')
        .upsert(payload, { onConflict: 'discord_user_id' })
        .select('id, discord_user_id')
        .single();

    if (error) throw error;
    return data;
}

async function ensureTagRows(tags = []) {
    const normalized = uniqueStrings(tags);
    if (!normalized.length) return new Map();

    const rows = normalized.map(name => ({ name }));

    const { error: upsertError } = await supabase
        .from('tags')
        .upsert(rows, { onConflict: 'name' });

    if (upsertError) throw upsertError;

    const { data, error } = await supabase
        .from('tags')
        .select('id, name')
        .in('name', normalized);

    if (error) throw error;

    const map = new Map();
    for (const row of data ?? []) {
        map.set(row.name, row.id);
    }
    return map;
}

// ====== コマンド登録 ======
async function registerCommands() {
    const gourmetCmd = new SlashCommandBuilder()
        .setName('gourmet')
        .setDescription('グルメ記録を開く');

    const rest = new REST({ version: '10' }).setToken(TOKEN);
    const guilds = await client.guilds.fetch();

    for (const [, g] of guilds) {
        await rest.put(
            Routes.applicationGuildCommands(client.user.id, g.id),
            { body: [gourmetCmd.toJSON()] }
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
    if (interaction.deferred) {
        return interaction.editReply(payload);
    }

    if (interaction.replied) {
        return interaction.editReply(payload);
    }

    return interaction.update(payload);
}

async function renderMineList(interaction, guildId, userId, { update = false } = {}) {
    const k = keyOf(guildId, userId);
    const st = mineState.get(k);

    if (!st?.results?.length) {
        const e = new EmbedBuilder()
            .setTitle('📚 自分の記録')
            .setDescription('(まだありません)');

        const payload = {
            embeds: [e],
            components: homeComponents(),
        };

        if (update) {
            return updateLike(interaction, payload);
        }

        await interaction.reply({
            flags: MessageFlags.Ephemeral,
            ...payload,
        });
        await rememberUiReply(interaction, guildId, userId);
        return;
    }

    const ownPosts = Array.isArray(st?.posts) ? st.posts : [];
    const postMap = new Map(ownPosts.map(p => [p.id, p]));

    const filteredIds = [];

    for (const pid of st.results) {
        const p = postMap.get(pid);
        if (!p) continue;
        if (!visitFilterMatch(st, p)) continue;
        filteredIds.push(pid);
    }

    if (!filteredIds.length) {
        const e = new EmbedBuilder()
            .setTitle('📚 自分の記録')
            .setDescription('(まだありません)');

        const payload = {
            embeds: [e],
            components: homeComponents(),
        };

        if (update) {
            return updateLike(interaction, payload);
        }

        await interaction.reply({
            flags: MessageFlags.Ephemeral,
            ...payload,
        });
        await rememberUiReply(interaction, guildId, userId);
        return;
    }

    const pageSize = 9;

    let page = Math.max(0, Number(st.page) || 0);
    const maxPage = Math.max(0, Math.ceil(filteredIds.length / pageSize) - 1);
    if (page > maxPage) page = maxPage;
    st.page = page;
    mineState.set(k, st);

    const start = page * pageSize;
    const sliceIds = filteredIds.slice(start, start + pageSize);
    const slice = [];

    for (const pid of sliceIds) {
        const p = postMap.get(pid);
        if (p) slice.push(p);
    }

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

    const payload = {
        embeds,
        components: comps,
    };

    if (update) {
        return updateLike(interaction, payload);
    }

    await interaction.reply({
        flags: MessageFlags.Ephemeral,
        ...payload,
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
            tagFilters: [],
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

        await interaction.reply({ flags: MessageFlags.Ephemeral, ...payload });
        await rememberUiReply(interaction, guildId, userId);
        return;
    }

    await ensureCacheLoadedForGuild(interaction.guild, userId);
    const cache = getGuildCache(guildId);

    const pageSize = 9;
    let page = Math.max(0, Number(st.page) || 0);
    const maxPage = Math.max(0, Math.ceil(st.results.length / pageSize) - 1);
    if (page > maxPage) page = maxPage;
    st.page = page;
    searchState.set(k, st);

    const start = page * pageSize;
    const sliceIds = st.results.slice(start, start + pageSize);
    const slice = [];

    for (const pid of sliceIds) {
        const p = await getPostByIdForViewer(pid, guildId, userId);
        if (p) slice.push(p);
    }

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

    await interaction.reply({ flags: MessageFlags.Ephemeral, ...payload });
    await rememberUiReply(interaction, guildId, userId);
}

function detailActionComponents(
    guildId,
    userId,
    postId,
    {
        fromMine = false,
        canEditThis = true,
        total = 1,
        forceHomeBack = false,
        hasPhotos = false,
        visibility = 'server',
        allowPublicPost = true,
    } = {}
) {
    const rows = [];

    if (canEditThis) {
        rows.push(
            visibilityComponents(guildId, userId, postId, visibility, allowPublicPost)
        );
    }

    const navRow = new ActionRowBuilder();

    if (hasPhotos) {
        navRow.addComponents(
            new ButtonBuilder()
                .setCustomId(`res:photoPrev:${guildId}:${userId}:${postId}`)
                .setLabel('◀ 写真')
                .setStyle(ButtonStyle.Secondary),
            new ButtonBuilder()
                .setCustomId(`res:photoNext:${guildId}:${userId}:${postId}`)
                .setLabel('写真 ▶')
                .setStyle(ButtonStyle.Secondary)
        );
    }

    navRow.addComponents(
        new ButtonBuilder()
            .setCustomId(forceHomeBack ? `home:back:${guildId}:${userId}` : (fromMine ? `mine:back:${guildId}:${userId}` : `search:back:${guildId}:${userId}`))
            .setLabel('戻る')
            .setStyle(ButtonStyle.Secondary)
    );

    rows.push(navRow);

    if (canEditThis) {
        rows.push(
            new ActionRowBuilder().addComponents(
                new ButtonBuilder()
                    .setCustomId(`res:edit:${guildId}:${userId}:${postId}`)
                    .setLabel('編集')
                    .setStyle(ButtonStyle.Primary),
                new ButtonBuilder()
                    .setCustomId(`res:delete:${guildId}:${userId}:${postId}`)
                    .setLabel('削除')
                    .setStyle(ButtonStyle.Danger)
            )
        );
    }

    return rows;
}

function visibilityComponents(
    guildId,
    userId,
    postId,
    currentVisibility = 'server',
    allowPublicPost = true
) {
    const current = currentVisibility || 'server';

    return new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId(`res:vis:private:${guildId}:${userId}:${postId}`)
            .setLabel(`${current === 'private' ? '◉' : '○'} 非公開`)
            .setStyle(ButtonStyle.Secondary),

        new ButtonBuilder()
            .setCustomId(`res:vis:server:${guildId}:${userId}:${postId}`)
            .setLabel(`${current === 'server' ? '◉' : '○'} サーバー公開`)
            .setStyle(ButtonStyle.Secondary),

        new ButtonBuilder()
            .setCustomId(`res:vis:public:${guildId}:${userId}:${postId}`)
            .setLabel(`${current === 'public' ? '◉' : '○'} 全体公開`)
            .setStyle(ButtonStyle.Secondary)
            .setDisabled(!allowPublicPost)
    );
}

function cameFromMine(k, postId, mineState) {
    const mine = mineState.get(k);
    return !!mine?.results?.includes(postId);
}

async function renderDetail(
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
        idx = Math.max(0, urls.length - 1);
        detailPhotoView.set(k, { postId: post.id, idx });
    }

    const detail = buildPostEmbedForView(post, { imageIndex: idx, notice });
    detail.setTitle(`📄 詳細  ${post.name}`.trim());

    const settings = await getServerSettingsByGuildId(guildId);

    const components = detailActionComponents(guildId, userId, post.id, {
        fromMine,
        canEditThis: canEdit(interaction, post),
        total,
        forceHomeBack,
        hasPhotos: urls.length > 1,
        visibility: post.visibility ?? 'server',
        allowPublicPost: settings.allow_public_post !== false,
    });

    return { detail, components };
}

async function deletePostImageFromStorage(imageRow) {
    if (!imageRow) return;

    if (imageRow.storage_path) {
        const { error: storageError } = await supabase.storage
            .from('post-images')
            .remove([imageRow.storage_path]);

        if (storageError) {
            throw new Error(`Storage削除失敗: ${storageError.message}`);
        }
    }

    const { error: dbError } = await supabase
        .from('post_images')
        .delete()
        .eq('id', imageRow.id);

    if (dbError) {
        throw new Error(`post_images削除失敗: ${dbError.message}`);
    }
}

async function deleteAllPostImagesFromStorage(postId) {
    const { data: images, error: fetchError } = await supabase
        .from('post_images')
        .select('id, storage_path')
        .eq('post_id', postId);

    if (fetchError) {
        throw new Error(`画像一覧取得失敗: ${fetchError.message}`);
    }

    const paths = (images ?? [])
        .map(x => x.storage_path)
        .filter(Boolean);

    if (paths.length) {
        const { error: storageError } = await supabase.storage
            .from('post-images')
            .remove(paths);

        if (storageError) {
            throw new Error(`Storage一括削除失敗: ${storageError.message}`);
        }
    }

    const { error: dbError } = await supabase
        .from('post_images')
        .delete()
        .eq('post_id', postId);

    if (dbError) {
        throw new Error(`post_images一括削除失敗: ${dbError.message}`);
    }
}

async function deletePostWithStorage(postId) {
    await deleteAllPostImagesFromStorage(postId);

    const { error: postError } = await supabase
        .from('posts')
        .delete()
        .eq('id', postId);

    if (postError) {
        throw new Error(`posts削除失敗: ${postError.message}`);
    }
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
            if (interaction.commandName === 'gourmet') {
                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

                try {
                    await ensureCacheLoadedForGuild(interaction.guild, userId);
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

            return;
        }

        // Buttons
        if (interaction.isButton()) {

            if (id === 'home:close') {
                await interaction.deferUpdate();
                await interaction.deleteReply().catch(() => { });
                return;
            }

            if (id.startsWith('place:prev:') || id.startsWith('place:next:')) {
                const [, action, gid, ownerId] = id.split(':');

                if (interaction.guildId !== gid) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
                }

                const st = placeSearchState.get(k);
                if (!st) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'お店検索状態がありません' });
                }

                const maxPage = Math.max(0, Math.ceil((st.results?.length || 0) / PLACE_PAGE_SIZE) - 1);
                st.page = Number(st.page) || 0;
                st.page += action === 'prev' ? -1 : 1;
                if (st.page < 0) st.page = 0;
                if (st.page > maxPage) st.page = maxPage;

                placeSearchState.set(k, st);

                return interaction.update({
                    content: '',
                    embeds: [buildPlaceSearchEmbed(st)],
                    components: placeSearchComponents(guildId, userId, st),
                });
            }

            if (id.startsWith('place:more:')) {
                const [, , gid, ownerId] = id.split(':');

                if (interaction.guildId !== gid) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
                }

                let st = placeSearchState.get(k);

                if (!st) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'お店検索状態がありません' });
                }

                if (!st.query) {
                    return interaction.reply({
                        flags: MessageFlags.Ephemeral,
                        content: '検索語が見つかりません。もう一度お店検索してください',
                    });
                }

                if (!st.nextPageToken) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これ以上候補がありません' });
                }

                st.loadingMore = true;
                placeSearchState.set(k, st);

                await interaction.update({
                    content: '',
                    embeds: [
                        new EmbedBuilder()
                            .setTitle('🍽 お店検索結果')
                            .setDescription('追加の候補を読み込み中です...')
                    ],
                    components: [],
                });

                try {
                    const more = await fetchMoreGooglePlaces(st.query, st.nextPageToken);

                    const exists = new Set((st.results ?? []).map(x => x.placeId));
                    for (const item of more.results) {
                        if (!exists.has(item.placeId)) {
                            st.results.push(item);
                            exists.add(item.placeId);
                        }
                    }

                    st.nextPageToken = more.nextPageToken ?? '';
                    st.loadingMore = false;
                    placeSearchState.set(k, st);

                    return interaction.editReply({
                        content: '',
                        embeds: [buildPlaceSearchEmbed(st)],
                        components: placeSearchComponents(guildId, userId, st),
                    });
                } catch (e) {
                    st.loadingMore = false;
                    placeSearchState.set(k, st);

                    return interaction.editReply({
                        content: `追加読込に失敗しました: ${e.message}`,
                        embeds: [buildPlaceSearchEmbed(st)],
                        components: placeSearchComponents(guildId, userId, st),
                    });
                }
            }

            if (id.startsWith('place:back:')) {
                const [, , gid, ownerId] = id.split(':');

                if (interaction.guildId !== gid) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
                }

                const st = placeSearchState.get(k);
                if (!st) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'お店検索状態がありません' });
                }

                placeSearchState.delete(k);

                if (st.mode === 'create') {
                    return renderCreatePanel(interaction, guildId, userId, { update: true });
                }

                return renderEditPanel(interaction, guildId, userId, { update: true });
            }

            if (id.startsWith('search:listPrev:') || id.startsWith('search:listNext:')) {
                const [, action, gid, ownerId] = id.split(':');

                if (interaction.guildId !== gid) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
                }

                const emptyState = {
                    userIdFilter: null,
                    prefectureFilters: [],
                    tagFilters: [],
                    keyword: '',
                    ratingFilters: [],
                    results: [],
                    page: 0,
                };

                const st = searchState.get(k);

                if (!st?.results?.length) {
                    return interaction.update({
                        content: '該当する記録がありません',
                        embeds: [searchPanelEmbed(st ?? emptyState)],
                        components: searchPanelComponents(guildId, userId, st ?? emptyState),
                    });
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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                await ensureCacheLoadedForGuild(interaction.guild, userId);
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

                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                await ensureCacheLoadedForGuild(interaction.guild, userId);
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

                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
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
                    await ensureCacheLoadedForGuild(interaction.guild, userId);
                    const post = await getPostByIdForViewer(wait.postId, guildId, userId);

                    if (post) {
                        const nav = getDetailNavState(guildId, userId, wait.postId);
                        const fromMine = nav?.fromMine ?? cameFromMine(k, wait.postId, mineState);
                        const forceHomeBack = nav?.forceHomeBack ?? false;

                        const { detail, components } = await renderDetail(interaction, {
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
                    openDetailAfterCreateState.set(k, { postId: wait.postId });

                    await interaction.update({
                        content: '',
                        embeds: [
                            new EmbedBuilder()
                                .setTitle('✅ 登録完了')
                                .setDescription('詳細画面を開きますか？')
                        ],
                        components: openDetailAfterCreateComponents(guildId, userId),
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
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
                }

                await ensureCacheLoadedForGuild(interaction.guild, userId);
                const cache = getGuildCache(guildId);
                const post = await getPostByIdForViewer(postId, guildId, userId);

                if (!post) {
                    return interaction.update({
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });
                }

                if (!canEdit(interaction, post)) {
                    return interaction.reply({
                        flags: MessageFlags.Ephemeral,
                        content: '削除できるのは登録者または管理者のみです'
                    });
                }

                const urls = imageUrls(post);
                if (!urls.length) {
                    if (kind === 'deleteAllPhotos') {
                        deleteAllPhotosConfirmState.set(k, { postId });

                        return interaction.update({
                            content: '',
                            embeds: [confirmDeleteAllPhotosEmbed(post)],
                            components: deleteAllPhotosConfirmComponents(guildId, ownerId),
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
                    deleteAllPhotosConfirmState.set(k, { postId });

                    const embed = new EmbedBuilder()
                        .setTitle('⚠ 写真をすべて削除')
                        .setDescription(`**${post.name}** の写真をすべて削除しますか？\n写真 ${idx + 1}/${urls.length}`)
                        .setImage(urls[idx]);

                    return interaction.update({
                        content: '',
                        embeds: [embed],
                        components: deleteAllPhotosConfirmComponents(guildId, ownerId),
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
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
                }

                const d = draftRating.get(k);
                if (!d || d.mode !== mode) {
                    return interaction.reply({
                        flags: MessageFlags.Ephemeral,
                        content: mode === 'create' ? '新規登録状態がありません' : '編集状態がありません'
                    });
                }

                await ensureCacheLoadedForGuild(interaction.guild, userId);
                const cache = getGuildCache(guildId);

                const existingTags = new Set(
                    getAllTagsFromCache(cache).map(x => String(x ?? '').trim().toLowerCase())
                );

                const pickedExistingTags = (interaction.values ?? [])
                    .map(x => String(x ?? '').trim())
                    .filter(Boolean);

                const customTags = (d.tags ?? [])
                    .filter(tag => !existingTags.has(String(tag ?? '').trim().toLowerCase()));

                d.tags = [...customTags, ...pickedExistingTags];
                draftRating.set(k, d);

                return mode === 'create'
                    ? renderCreatePanel(interaction, guildId, userId, { update: true })
                    : renderEditPanel(interaction, guildId, userId, { update: true });
            }
        }

        if (id.startsWith('search:tagPick:')) {
            const parts = id.split(':');
            const gid = parts[2];
            const ownerId = parts[3];
            const page = Number(parts[4] || 0);

            if (interaction.guildId !== gid) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
            }

            const picked = interaction.values ?? [];

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                tagFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            await ensureCacheLoadedForGuild(interaction.guild, userId);
            const cache = getGuildCache(guildId);

            const { slice } = tagSlice(cache, page);
            const current = new Set(st.tagFilters ?? []);

            // 今のページにあるタグはいったん外す
            for (const tag of slice) {
                current.delete(tag);
            }

            // 今回選んだタグを追加
            for (const tag of picked) {
                current.add(tag);
            }

            st.tagFilters = [...current];
            searchState.set(k, st);

            return interaction.update({
                content: st.tagFilters.length
                    ? `タグを設定しました: ${st.tagFilters.map(x => `#${x}`).join(' / ')}`
                    : 'タグを解除しました',
                embeds: [searchPanelEmbed(st)],
                components: searchPanelComponents(guildId, userId, st),
            });
        }

        if (id.startsWith('confirm:')) {
            const [, answer, kind, gid, ownerId, postId, extra] = id.split(':');

            if (interaction.guildId !== gid) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
            }

            await ensureCacheLoadedForGuild(interaction.guild, userId);
            const cache = getGuildCache(guildId);
            const post = postId
                ? await getPostByIdForViewer(postId, guildId, userId)
                : null;

            if (kind === 'dp') {
                const st = deletePhotoConfirmState.get(k);

                if (answer === 'no' || !st?.postId) {
                    deletePhotoConfirmState.delete(k);

                    await interaction.update({
                        content: '',
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });
                    uiMessages.set(k, new Set([interaction.message.id]));
                    return;
                }

                const targetPost = await getPostByIdForViewer(st.postId, guildId, userId);
                if (!targetPost) {
                    deletePhotoConfirmState.delete(k);

                    await interaction.update({
                        content: '',
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });
                    uiMessages.set(k, new Set([interaction.message.id]));
                    return;
                }

                if (!canEdit(interaction, targetPost)) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '写真の編集は投稿者のみです' });
                }

                const imgs = targetPost.images ?? [];
                if (!imgs.length) {
                    deletePhotoConfirmState.delete(k);
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '写真がありません' });
                }

                const idx = Math.max(0, Math.min(imgs.length - 1, Number(st.idx) || 0));
                const target = imgs[idx];

                if (!target?.id) {
                    deletePhotoConfirmState.delete(k);
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '写真IDが見つかりません' });
                }

                await deletePostImageWithStorage(target);

                const fresh = await refreshPostCacheById(st.postId, guildId);
                deletePhotoConfirmState.delete(k);

                if (!fresh) {
                    return interaction.update({
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });
                }

                const urls2 = imageUrls(fresh);
                const newIdx = Math.max(0, Math.min(idx, urls2.length - 1));
                photoView.set(k, { postId: st.postId, idx: newIdx });

                const embed = new EmbedBuilder()
                    .setTitle(`🖼 写真管理: ${safeText(fresh.name || '(名称不明)', 200)}`)
                    .setDescription(urls2.length ? `写真 ${newIdx + 1}/${urls2.length}` : '写真はありません');

                if (urls2.length && urls2[newIdx]) {
                    embed.setImage(urls2[newIdx]);
                }

                return interaction.update({
                    embeds: [embed],
                    components: photoManagerComponents(guildId, ownerId, st.postId, urls2.length),
                });
            }

            if (kind === 'da') {
                const st = deleteAllPhotosConfirmState.get(k);

                if (answer === 'no' || !st?.postId) {
                    deleteAllPhotosConfirmState.delete(k);

                    await interaction.update({
                        content: '',
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });
                    uiMessages.set(k, new Set([interaction.message.id]));
                    return;
                }

                const targetPost = await getPostByIdForViewer(st.postId, guildId, userId);
                if (!targetPost) {
                    deleteAllPhotosConfirmState.delete(k);

                    await interaction.update({
                        content: '',
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });
                    uiMessages.set(k, new Set([interaction.message.id]));
                    return;
                }

                if (!canEdit(interaction, targetPost)) {
                    return interaction.reply({
                        flags: MessageFlags.Ephemeral,
                        content: '操作できるのは登録者または管理者のみです'
                    });
                }

                await deleteAllPostImagesWithStorage(st.postId);

                const fresh = await refreshPostCacheById(st.postId, guildId);
                deleteAllPhotosConfirmState.delete(k);

                if (!fresh) {
                    return interaction.update({
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });
                }

                photoView.set(k, { postId: st.postId, idx: 0 });

                const embed = new EmbedBuilder()
                    .setTitle(`🖼 写真管理: ${safeText(fresh.name || '(名称不明)', 200)}`)
                    .setDescription('写真はありません');

                return interaction.update({
                    embeds: [embed],
                    components: photoManagerComponents(guildId, ownerId, st.postId, 0),
                });
            }

            if (kind === 'od') {
                const st = openDetailAfterCreateState.get(k);

                if (answer === 'no' || !st?.postId) {
                    openDetailAfterCreateState.delete(k);

                    await interaction.update({
                        content: '',
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });

                    uiMessages.set(k, new Set([interaction.message.id]));
                    return;
                }

                const targetPost = await getPostByIdForViewer(st.postId, guildId, userId);
                openDetailAfterCreateState.delete(k);

                if (!targetPost) {
                    await interaction.update({
                        content: '',
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });

                    uiMessages.set(k, new Set([interaction.message.id]));
                    return;
                }

                const { detail, components } = await renderDetail(interaction, {
                    post: targetPost,
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

            // ===== NO =====
            if (answer === 'no') {
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
                        .setTitle(`🖼 写真管理: ${safeText(post.name || '(名称不明)', 200)}`)
                        .setDescription(urls.length ? `写真 ${idx + 1}/${urls.length}` : '写真はありません');

                    if (urls.length && urls[idx]) {
                        embed.setImage(urls[idx]);
                    }

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

                    const { detail, components } = await renderDetail(interaction, {
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

                return interaction.update({
                    content: '',
                    embeds: [homeEmbed()],
                    components: homeComponents(),
                });
            }

            // ===== YES =====
            if (!post && kind !== 'deletePost') {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'データが見つかりません' });
            }

            if (kind === 'deletePost') {
                if (!post) {
                    return interaction.update({
                        embeds: [homeEmbed()],
                        components: homeComponents(),
                    });
                }

                if (!canEdit(interaction, post)) {
                    return interaction.reply({
                        flags: MessageFlags.Ephemeral,
                        content: '削除できるのは登録者または管理者のみです'
                    });
                }

                const fromMine = cameFromMine(k, postId, mineState);

                await deletePostWithImagesAndStorage(postId);

                getGuildCache(guildId).delete(postId);

                const mine = mineState.get(k);
                if (mine?.results) {
                    mine.results = mine.results.filter(x => x !== postId);
                    mine.posts = (mine.posts ?? []).filter(x => x.id !== postId);
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
                    return renderSearchResultList(interaction, guildId, userId, { update: true });
                }

                return interaction.update({
                    embeds: [homeEmbed()],
                    components: homeComponents(),
                });
            }

            return interaction.update({
                content: '',
                embeds: [homeEmbed()],
                components: homeComponents(),
            });
        }

        if (id.startsWith('date:input:')) {
            const [, action, gid, ownerId, mode, postId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== mode) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '途中状態がありません最初からやり直してください' });
            }

            const currentValue = (d.visitedDate ?? '').trim() || todayYMD();

            return interaction.showModal(
                buildVisitedDateModal(gid, ownerId, mode, postId || '', currentValue)
            );
        }

        if (id.startsWith('search:prefPagePrev:') || id.startsWith('search:prefPageNext:')) {
            const parts = id.split(':');
            const gid = parts[2];
            const ownerId = parts[3];
            const page = Number(parts[4] || 0);

            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const next = id.includes('prefPageNext') ? page + 1 : page - 1;
            const { p, totalPages, slice } = prefSlice(next);

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                tagFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            const select = new StringSelectMenuBuilder()
                .setCustomId(`search:prefPick:${gid}:${ownerId}:${p}`)
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
                    .setCustomId(`search:prefPagePrev:${gid}:${ownerId}:${p}`)
                    .setLabel('◀ 前へ')
                    .setStyle(ButtonStyle.Secondary)
                    .setDisabled(p <= 0),

                new ButtonBuilder()
                    .setCustomId(`search:prefPageNext:${gid}:${ownerId}:${p}`)
                    .setLabel('次へ ▶')
                    .setStyle(ButtonStyle.Secondary)
                    .setDisabled(p >= totalPages - 1),

                new ButtonBuilder()
                    .setCustomId(`search:prefPageClear:${gid}:${ownerId}`)
                    .setLabel('解除')
                    .setStyle(ButtonStyle.Secondary),

                new ButtonBuilder()
                    .setCustomId(`search:prefBack:${gid}:${ownerId}`)
                    .setLabel('戻る')
                    .setStyle(ButtonStyle.Secondary),
            );

            return interaction.update({
                content: '都道府県を選択してください',
                embeds: [],
                components: [new ActionRowBuilder().addComponents(select), nav],
            });
        }
        if (id.startsWith('search:prefPageClear:')) {
            const parts = id.split(':');
            const gid = parts[2];
            const ownerId = parts[3];

            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                tagFilters: [],
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

            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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

        if (id.startsWith('create:searchPlace:') || id.startsWith('edit:searchPlace:')) {
            const parts = id.split(':');
            const mode = parts[0];
            const gid = parts[2];
            const ownerId = parts[3];

            if (interaction.guildId !== gid) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
            }

            const d = draftRating.get(k);
            if (!d || d.mode !== mode) {
                return interaction.reply({
                    flags: MessageFlags.Ephemeral,
                    content: mode === 'create' ? '新規登録状態がありません' : '編集状態がありません'
                });
            }

            placeSearchState.set(k, {
                mode,
                query: d.name ?? '',
                results: [],
                page: 0,
                nextPageToken: '',
                loadingMore: false,
            });

            return interaction.showModal(
                buildPlaceSearchModal(gid, ownerId, mode, d.name ?? '')
            );
        }

        if (id.startsWith('create:setName:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
            }

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildNameModal(gid, ownerId, d.name ?? ''));
        }

        // Home
        if (id.startsWith('create:setVisit:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== 'create') {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '新規登録状態がありません' });
            }

            d.visited = kind === 'visited';

            if (d.visited === false) {
                d.rating = null;
                d.visitedDate = '';
            }

            draftRating.set(k, d);
            return renderCreatePanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('create:setRating:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== 'create') {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '新規登録状態がありません' });
            }

            d.rating = Number(ratingStr);
            draftRating.set(k, d);

            return renderCreatePanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('create:setComment:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildCommentModal(gid, ownerId, d.comment ?? ''));
        }

        if (id.startsWith('create:setTags:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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

            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild, userId);
            const cache = getGuildCache(guildId);

            const d = draftRating.get(k);
            if (!d || d.mode !== mode) {
                return interaction.reply({
                    flags: MessageFlags.Ephemeral,
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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildTagsModal(gid, ownerId, (d.tags ?? []).join(', ')));
        }

        if (id.startsWith('edit:tagsNew:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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

            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild, userId);
            const cache = getGuildCache(guildId);

            const d = draftRating.get(k);
            if (!d || d.mode !== mode) {
                return interaction.reply({
                    flags: MessageFlags.Ephemeral,
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

            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== mode) {
                return interaction.reply({
                    flags: MessageFlags.Ephemeral,
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
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
            }

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('🏷 タグ').setDescription('入力方法を選択してください')],
                components: tagEntryChoiceComponents(mode, gid, ownerId),
            });
        }

        if (id.startsWith('create:setUrl:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildUrlModal(gid, ownerId, d.url ?? ''));
        }

        if (id.startsWith('create:setMap:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildMapModal(gid, ownerId, d.mapUrl ?? ''));
        }

        if (id.startsWith('create:setDate:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            const currentValue = (d.visitedDate ?? '').trim() || todayYMD();

            return interaction.showModal(
                buildVisitedDateModal(gid, ownerId, 'create', '', currentValue)
            );
        }

        if (id.startsWith('create:setPref:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            return renderCreatePanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('create:reset:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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
                visibility: 'server',
                visibleServerIds: [guildId],
            });

            return renderCreatePanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('create:cancel:') || id.startsWith('create:home:')) {
            const [, action, gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            draftRating.delete(k);

            return interaction.update({
                content: '',
                embeds: [homeEmbed()],
                components: homeComponents(),
            });
        }

        if (id.startsWith('create:submit:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
            }

            const d = draftRating.get(k);
            if (!d || d.mode !== 'create') {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '新規登録状態がありません' });
            }

            if (!d.name || !d.name.trim()) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'お店の名前を入力してください' });
            }

            if (d.visited == null) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '訪問状態を選択してください' });
            }

            if (d.visited === true && (d.rating == null || ![1, 2, 3, 4, 5].includes(Number(d.rating)))) {
                return interaction.reply({
                    flags: MessageFlags.Ephemeral,
                    content: '「行った」を選んだ場合は評価を選択してください'
                });
            }

            await interaction.deferUpdate();

            const createdPost = await createPostInDb(interaction.guild, userId, d);
            const post = await refreshPostCacheById(createdPost.id, guildId);

            if (!post) {
                return interaction.editReply({
                    content: '作成後の投稿取得に失敗しました',
                    embeds: [],
                    components: [],
                });
            }

            const mine = mineState.get(k);
            if (mine?.results) {
                mine.results = [post.id, ...mine.results.filter(x => x !== post.id)];
                mine.posts = [post, ...(mine.posts ?? []).filter(x => x.id !== post.id)];
                mineState.set(k, mine);
            }

            draftRating.delete(k);
            createPanelPromptRef.delete(k);

            await interaction.editReply({
                content: '',
                embeds: [photoCreateWaitingEmbed(post.name ?? '(名称不明)')],
                components: photoWaitingComponents(guildId, userId),
            });

            const waitingMsg = await interaction.fetchReply().catch(() => null);

            awaitingPhoto.set(k, {
                postId: post.id,
                channelId: interaction.channelId,
                guildId,
                backTo: 'home',
                uiMessageRef: waitingMsg?.id ? {
                    webhook: interaction.webhook,
                    messageId: waitingMsg.id,
                } : null,
            });

            return;
        }

        if (id.startsWith('edit:setVisit:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
            }

            const d = draftRating.get(k);
            if (!d || d.mode !== 'edit') {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '編集状態がありません' });
            }

            return interaction.showModal(buildEditNameModal(gid, ownerId, d.name ?? ''));
        }

        if (id.startsWith('edit:visit:')) {
            const [, , kind, gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== 'edit') {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '編集状態がありません' });
            }

            d.visited = kind === 'visited';

            if (d.visited === false) {
                d.rating = null;
                d.visitedDate = '';
            }

            draftRating.set(k, d);
            return renderEditPanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('edit:setRating:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== 'edit') {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '編集状態がありません' });
            }

            d.rating = Number(ratingStr);
            draftRating.set(k, d);

            return renderEditPanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('edit:setComment:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildEditCommentModal(gid, ownerId, d.comment ?? ''));
        }

        if (id.startsWith('edit:setTags:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            return interaction.update({
                content: '',
                embeds: [new EmbedBuilder().setTitle('🏷 タグ').setDescription('入力方法を選択してください')],
                components: tagEntryChoiceComponents('edit', gid, ownerId),
            });
        }

        if (id.startsWith('edit:setUrl:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildEditUrlModal(gid, ownerId, d.url ?? ''));
        }

        if (id.startsWith('edit:setMap:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            return interaction.showModal(buildEditMapModal(gid, ownerId, d.mapUrl ?? ''));
        }

        if (id.startsWith('edit:setDate:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k) ?? {};
            const currentValue = (d.visitedDate ?? '').trim() || todayYMD();

            return interaction.showModal(
                buildVisitedDateModal(gid, ownerId, 'edit', '', currentValue)
            );
        }

        if (id.startsWith('edit:setPref:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            return renderEditPanel(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('edit:back:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
            }

            const d = draftRating.get(k);
            if (!d || d.mode !== 'edit' || !d.postId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '編集状態がありません' });
            }

            await ensureCacheLoadedForGuild(interaction.guild, userId);
            const post = await getPostByIdForViewer(d.postId, guildId, userId);

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

            const { detail, components } = await renderDetail(interaction, {
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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== 'edit' || !d.postId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '編集状態がありません' });
            }

            await ensureCacheLoadedForGuild(interaction.guild, userId);
            const post = await getPostByIdForViewer(d.postId, guildId, userId);
            if (!post) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'データが見つかりません' });

            const urls = imageUrls(post);
            photoView.set(k, { postId: post.id, idx: Math.max(0, urls.length - 1) });

            const idx = Math.max(0, urls.length - 1);

            const embed = new EmbedBuilder()
                .setTitle(`🖼 写真管理: ${safeText(post.name || '(名称不明)', 200)}`)
                .setDescription(urls.length ? `写真 ${idx + 1}/${urls.length}` : '写真はありません');

            if (urls.length && urls[idx]) {
                embed.setImage(urls[idx]);
            }

            return interaction.update({
                embeds: [embed],
                components: photoManagerComponents(guildId, ownerId, post.id, urls.length),
            });
        }

        if (id.startsWith('edit:submit:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const d = draftRating.get(k);
            if (!d || d.mode !== 'edit' || !d.postId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '編集状態がありません' });
            }

            if (!d.name?.trim()) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'お店の名前は必須です' });
            }

            if (d.visited == null) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '訪問状態を選択してください' });
            }

            if (d.visited === true && (d.rating == null || ![1, 2, 3, 4, 5].includes(Number(d.rating)))) {
                return interaction.reply({
                    flags: MessageFlags.Ephemeral,
                    content: '「行った」を選んだ場合は評価を選択してください'
                });
            }

            await interaction.deferUpdate();

            await ensureCacheLoadedForGuild(interaction.guild, userId);
            const post = await getPostByIdForViewer(d.postId, guildId, userId);

            if (!post) {
                return interaction.editReply({
                    content: '対象データが見つかりません',
                    embeds: [],
                    components: [],
                });
            }

            if (!canEdit(interaction, post)) {
                return interaction.editReply({
                    content: '操作できるのは登録者または管理者のみです',
                    embeds: [],
                    components: [],
                });
            }

            await updatePostInDb(interaction.guild, userId, d);

            const fresh = await refreshPostCacheById(d.postId, guildId);

            if (!fresh) {
                return interaction.editReply({ content: '更新後データが見つかりません', embeds: [], components: [] });
            }

            draftRating.delete(k);
            editPanelPromptRef.delete(k);

            const nav = getDetailNavState(guildId, userId, fresh.id);
            const fromMine = nav?.fromMine ?? cameFromMine(k, fresh.id, mineState);
            const forceHomeBack = nav?.forceHomeBack ?? false;

            const { detail, components } = await renderDetail(interaction, {
                post: fresh,
                guildId,
                userId,
                fromMine,
                total: forceHomeBack ? 1 : (fromMine ? 1 : (searchState.get(k)?.results?.length || 1)),
                forceHomeBack,
            });

            await interaction.editReply({
                content: '',
                embeds: [detail],
                components,
            });

            await interaction.followUp({
                content: '✅ 更新しました',
                flags: MessageFlags.Ephemeral,
            });

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
                visibility: 'server',
                visibleServerIds: [guildId],
            });

            await renderCreatePanel(interaction, guildId, userId, { update: true });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        if (id === 'home:search') {
            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                tagFilters: [],
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

            const currentMine = mineState.get(k);
            const needReload =
                !currentMine?.results?.length ||
                !Array.isArray(currentMine?.posts) ||
                currentMine.posts.length === 0;

            if (needReload) {
                const { data: userRow, error: userErr } = await supabase
                    .from('users')
                    .select('id')
                    .eq('discord_user_id', userId)
                    .maybeSingle();

                if (userErr) throw userErr;

                if (!userRow) {
                    mineState.set(k, {
                        results: [],
                        posts: [],
                        page: 0,
                        visitFilter: currentMine?.visitFilter ?? 'all',
                    });

                    await renderMineList(interaction, guildId, userId, { update: true });
                    await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
                    return;
                }

                const { data, error } = await supabase
                    .from('posts')
                    .select(`
                id,
                server_id,
                user_id,
                shop_id,
                shop_name,
                shop_prefecture,
                shop_map_url,
                shop_website_url,
                visited,
                rating,
                comment,
                visited_date,
                visibility,
                created_at,
                updated_at,
                users!posts_user_id_fkey (
                    id,
                    discord_user_id,
                    name
                ),
                post_images (
                    id,
                    image_url,
                    storage_path,
                    sort_order
                ),
                post_tags (
                    tag_id,
                    tags (
                        id,
                        name
                    )
                ),
                post_visible_servers (
                    server_id,
                    servers (
                        id,
                        discord_server_id,
                        name
                    )
                )
            `)
                    .eq('user_id', userRow.id)
                    .order('created_at', { ascending: false });

                if (error) throw error;

                const minePosts = (data ?? []).map(mapDbPostToView);

                const cache = getGuildCache(guildId);
                for (const post of minePosts) {
                    cache.set(post.id, post);
                }

                mineState.set(k, {
                    results: minePosts.map(p => p.id),
                    posts: minePosts,
                    page: 0,
                    visitFilter: currentMine?.visitFilter ?? 'all',
                });
            } else {
                currentMine.page = 0;
                currentMine.visitFilter = currentMine.visitFilter ?? 'all';
                mineState.set(k, currentMine);
            }

            await renderMineList(interaction, guildId, userId, { update: true });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        // Search panel
        if (id.startsWith('search:setUser:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                tagFilters: [],
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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                tagFilters: [],
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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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

        if (id.startsWith('search:prefBack:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const { p, totalPages, slice } = prefSlice(0);

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                tagFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };

            const select = new StringSelectMenuBuilder()
                .setCustomId(`search:prefPick:${gid}:${ownerId}:${p}`)
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
                    .setCustomId(`search:prefPagePrev:${gid}:${ownerId}:${p}`)
                    .setLabel('◀ 前へ')
                    .setStyle(ButtonStyle.Secondary)
                    .setDisabled(p <= 0),

                new ButtonBuilder()
                    .setCustomId(`search:prefPageNext:${gid}:${ownerId}:${p}`)
                    .setLabel('次へ ▶')
                    .setStyle(ButtonStyle.Secondary)
                    .setDisabled(p >= totalPages - 1),

                new ButtonBuilder()
                    .setCustomId(`search:prefPageClear:${gid}:${ownerId}`)
                    .setLabel('解除')
                    .setStyle(ButtonStyle.Secondary),

                new ButtonBuilder()
                    .setCustomId(`search:prefBack:${gid}:${ownerId}`)
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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                tagFilters: [],
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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const st = {
                userIdFilter: null,
                prefectureFilters: [],
                tagFilters: [],
                keyword: '',
                ratingFilters: [],
                results: [],
                page: 0
            };
            searchState.set(k, st);
            return interaction.update({ embeds: [searchPanelEmbed(st)], components: searchPanelComponents(guildId, userId, st) });
        }

        if (id.startsWith('search:back:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            await interaction.update({ embeds: [homeEmbed()], components: homeComponents() });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        if (id.startsWith('search:run:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            await interaction.deferUpdate();

            await ensureCacheLoadedForGuild(interaction.guild, userId);

            const serverRow = await ensureServerRowByGuildId(guildId);
            const cache = getGuildCache(guildId);
            const privatePosts = await getPrivatePostsForViewer(guildId, userId);

            const merged = new Map();

            for (const post of cache.values()) {
                merged.set(post.id, post);
            }
            for (const post of privatePosts) {
                merged.set(post.id, post);
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

            const results = [];

            for (const p of merged.values()) {
                if (!canViewPost({
                    viewerDiscordUserId: userId,
                    viewerDiscordGuildId: guildId,
                    viewerServerRowId: serverRow?.id ?? null,
                    post: p,
                })) continue;

                if (st.userIdFilter?.length) {
                    if (!st.userIdFilter.includes(p.created_by)) continue;
                }

                if (st.prefectureFilters?.length) {
                    const pp = (p.prefecture ?? '').trim();
                    if (!st.prefectureFilters.includes(pp)) continue;
                }

                if (st.tagFilters?.length) {
                    const tags = (p.tags ?? [])
                        .map(x => String(x ?? '').normalize('NFKC').toLowerCase().trim())
                        .filter(Boolean);

                    const selectedTags = (st.tagFilters ?? [])
                        .map(x => String(x ?? '').normalize('NFKC').toLowerCase().trim())
                        .filter(Boolean);

                    if (selectedTags.length && !selectedTags.some(tag => tags.includes(tag))) {
                        continue;
                    }
                }

                if ((st.keyword ?? '').trim()) {
                    const keywords = (st.keyword ?? '')
                        .normalize('NFKC')
                        .toLowerCase()
                        .trim()
                        .split(/[ \u3000]+/)
                        .filter(Boolean);

                    const hay = [
                        p.name ?? '',
                        p.comment ?? '',
                        p.prefecture ?? '',
                        ...(p.tags ?? [])
                    ]
                        .join('\n')
                        .normalize('NFKC')
                        .toLowerCase();

                    let ok = true;
                    for (const kw of keywords) {
                        if (!hay.includes(kw)) {
                            ok = false;
                            break;
                        }
                    }
                    if (!ok) continue;
                }

                if (st.ratingFilters?.length) {
                    if (!st.ratingFilters.includes(Number(p.rating))) continue;
                }

                results.push(p);
            }

            results.sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''));
            st.results = results.map(p => p.id);
            st.page = 0;
            searchState.set(k, st);

            if (!st.results.length) {
                await interaction.editReply({
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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const rating = Number(ratingStr);
            const st = searchState.get(k) ?? {
                userIdFilter: null,
                prefectureFilters: [],
                tagFilters: [],
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

        if (id.startsWith('res:photoPrev:') || id.startsWith('res:photoNext:')) {
            const [, action, gid, ownerId, postId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild, userId);

            const mine = mineState.get(k);
            const minePostMap = new Map((mine?.posts ?? []).map(p => [p.id, p]));
            const post = minePostMap.get(postId) ?? await getPostByIdForViewer(postId, guildId, userId);

            if (!post) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'データが見つかりません' });

            const urls = imageUrls(post);
            if (urls.length <= 1) {
                const nav = getDetailNavState(guildId, userId, postId);
                const fromMine = nav?.fromMine ?? cameFromMine(k, postId, mineState);
                const forceHomeBack = nav?.forceHomeBack ?? false;

                const { detail, components } = await renderDetail(interaction, {
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

            if (action === 'photoPrev') {
                pv.idx = (pv.idx - 1 + urls.length) % urls.length;
            } else {
                pv.idx = (pv.idx + 1) % urls.length;
            }

            detailPhotoView.set(k, pv);

            const nav = getDetailNavState(guildId, userId, postId);
            const fromMine = nav?.fromMine ?? cameFromMine(k, postId, mineState);
            const forceHomeBack = nav?.forceHomeBack ?? false;

            const { detail, components } = await renderDetail(interaction, {
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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            cacheReadyByGuild.set(guildId, false);
            await ensureCacheLoadedForGuild(interaction.guild, userId);
            const post = await getPostByIdForViewer(postId, guildId, userId);
            if (!post) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'データが見つかりません' });

            const chunks = buildDetailEmbedsChunks(post, { sharedByUserId: userId });
            for (const embeds of chunks) {
                await interaction.channel.send({ embeds });
            }

            const nav = getDetailNavState(guildId, userId, postId);
            const fromMine = nav?.fromMine ?? cameFromMine(k, postId, mineState);
            const forceHomeBack = nav?.forceHomeBack ?? false;

            const { detail, components } = await renderDetail(interaction, {
                post,
                guildId,
                userId,
                fromMine,
                total: forceHomeBack ? 1 : (fromMine ? 1 : (searchState.get(k)?.results?.length || 1)),
                forceHomeBack,
                notice: 'このチャンネルに送信しました',
            });

            await interaction.update({
                content: '',
                embeds: [detail],
                components,
            });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        if (id.startsWith('res:vis:')) {
            const [, , vis, gid, ownerId, postId] = id.split(':');

            if (interaction.guildId !== gid) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
            }

            if (!['private', 'server', 'public'].includes(vis)) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '公開範囲が不正です' });
            }

            const settings = await getServerSettingsByGuildId(guildId);
            if (vis === 'public' && settings.allow_public_post === false) {
                return interaction.reply({
                    flags: MessageFlags.Ephemeral,
                    content: 'このサーバーでは全体公開は許可されていません'
                });
            }

            await interaction.deferUpdate();

            await ensureCacheLoadedForGuild(interaction.guild, userId);
            const cache = getGuildCache(guildId);

            const mine = mineState.get(k);
            const minePostMap = new Map((mine?.posts ?? []).map(p => [p.id, p]));
            const post = minePostMap.get(postId) ?? await getPostByIdForViewer(postId, guildId, userId);

            if (!post) {
                return interaction.followUp({ flags: MessageFlags.Ephemeral, content: 'データが見つかりません' });
            }

            if (!canEdit(interaction, post)) {
                return interaction.followUp({ flags: MessageFlags.Ephemeral, content: '変更できるのは投稿者のみです' });
            }

            const updatedAt = nowIso();

            const { error } = await supabase
                .from('posts')
                .update({
                    visibility: vis,
                    updated_at: updatedAt
                })
                .eq('id', postId);

            if (error) throw error;

            await replacePostVisibleServers(
                postId,
                vis === 'server' ? [guildId] : []
            );

            const d = draftRating.get(k);
            if (d && d.mode === 'edit' && d.postId === postId) {
                d.visibility = vis;
                d.visibleServerIds = vis === 'server' ? [guildId] : [];
                draftRating.set(k, d);
            }

            const fresh = await refreshPostCacheById(postId, guildId);

            if (!fresh) {
                return interaction.editReply({
                    content: '',
                    embeds: [homeEmbed()],
                    components: homeComponents(),
                });
            }

            if (mine?.posts) {
                mine.posts = [fresh, ...mine.posts.filter(x => x.id !== postId)];
                mine.results = [fresh.id, ...((mine.results ?? []).filter(x => x !== postId))];
                mineState.set(k, mine);
            }

            const serverRow = await ensureServerRowByGuildId(guildId);

            if (!canViewPost({
                viewerDiscordUserId: userId,
                viewerDiscordGuildId: guildId,
                viewerServerRowId: serverRow?.id ?? null,
                post: fresh,
            })) {
                cache.delete(postId);

                return interaction.editReply({
                    content: '',
                    embeds: [homeEmbed()],
                    components: homeComponents(),
                });
            }

            cache.set(postId, fresh);

            const nav = getDetailNavState(guildId, userId, postId);
            const fromMine = nav?.fromMine ?? cameFromMine(k, postId, mineState);
            const forceHomeBack = nav?.forceHomeBack ?? false;

            const { detail, components } = await renderDetail(interaction, {
                post: fresh,
                guildId,
                userId,
                fromMine,
                total: forceHomeBack ? 1 : (fromMine ? 1 : (searchState.get(k)?.results?.length || 1)),
                forceHomeBack,
            });

            return interaction.editReply({
                content: '',
                embeds: [detail],
                components,
            });
        }

        // Edit from result (only if canEdit)
        if (id.startsWith('res:edit:')) {
            const [, , gid, ownerId, postId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild, userId);

            const mine = mineState.get(k);
            const minePostMap = new Map((mine?.posts ?? []).map(p => [p.id, p]));
            const post = minePostMap.get(postId) ?? await getPostByIdForViewer(postId, guildId, userId);

            if (!post) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'データが見つかりません' });
            if (!canEdit(interaction, post)) return interaction.reply({ flags: MessageFlags.Ephemeral, content: '編集できません（投稿者のみ）' });

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
                visibility: post.visibility ?? 'server',
                visibleServerIds: post.visible_server_ids?.length ? post.visible_server_ids : [guildId],
                channelId: interaction.channelId,
            });

            await renderEditPanel(interaction, guildId, userId, { update: true });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        // Photos from result (only if canEdit)
        if (id.startsWith('res:photos:')) {
            const [, , gid, ownerId, postId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild, userId);

            const mine = mineState.get(k);
            const minePostMap = new Map((mine?.posts ?? []).map(p => [p.id, p]));
            const post = minePostMap.get(postId) ?? await getPostByIdForViewer(postId, guildId, userId);

            if (!post) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'データが見つかりません' });
            if (!canEdit(interaction, post)) return interaction.reply({ flags: MessageFlags.Ephemeral, content: '写真の編集は投稿者のみです' });

            const urls = imageUrls(post);

            photoView.set(k, { postId, idx: Math.max(0, urls.length - 1) });
            const pv = photoView.get(k);
            const total = urls.length;

            const embed = new EmbedBuilder()
                .setTitle(`🖼 写真管理: ${safeText(post.name || '(名称不明)', 200)}`)
                .setDescription(total ? `写真 ${pv.idx + 1}/${total}` : '写真はありません');

            if (total && urls[pv.idx]) {
                embed.setImage(urls[pv.idx]);
            }

            return interaction.update({
                embeds: [embed],
                components: photoManagerComponents(guildId, ownerId, postId, total),
            });
        }

        if (id.startsWith('res:delete:')) {
            const [, , gid, ownerId, postId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild, userId);

            const mine = mineState.get(k);
            const minePostMap = new Map((mine?.posts ?? []).map(p => [p.id, p]));
            const post = minePostMap.get(postId) ?? await getPostByIdForViewer(postId, guildId, userId);

            if (!post) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'データが見つかりません' });
            if (!canEdit(interaction, post)) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '削除できるのは登録者または管理者のみです' });
            }

            const urls = imageUrls(post);
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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            return renderSearchResultList(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('search:backToPanel:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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

        if (id.startsWith('search:home:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            await ensureCacheLoadedForGuild(interaction.guild, userId);

            const mine = mineState.get(k);
            const minePostMap = new Map((mine?.posts ?? []).map(p => [p.id, p]));
            const post = minePostMap.get(postId) ?? await getPostByIdForViewer(postId, guildId, userId);

            if (!post) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'データが見つかりません' });
            if (!canEdit(interaction, post)) return interaction.reply({ flags: MessageFlags.Ephemeral, content: '写真の編集は投稿者のみです' });

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
                if (!urls.length) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '写真がありません' });
                }

                const idx = Math.max(0, Math.min(urls.length - 1, Number(pv.idx) || 0));

                deletePhotoConfirmState.set(k, { postId, idx });

                return interaction.update({
                    content: '',
                    embeds: [confirmPhotoDeleteEmbed(post, idx)],
                    components: deletePhotoConfirmComponents(guildId, ownerId),
                });
            }

            if (action === 'delall') {
                const imgs = post.images ?? [];

                if (!imgs.length) {
                    return interaction.reply({
                        flags: MessageFlags.Ephemeral,
                        content: '写真がありません'
                    });
                }

                deleteAllPhotosConfirmState.set(k, { postId });

                return interaction.update({
                    content: '',
                    embeds: [confirmDeleteAllPhotosEmbed(post)],
                    components: deleteAllPhotosConfirmComponents(guildId, ownerId),
                });
            }

            if (action === 'back') {
                const nav = getDetailNavState(guildId, userId, postId);
                const fromMine = nav?.fromMine ?? cameFromMine(k, postId, mineState);
                const forceHomeBack = nav?.forceHomeBack ?? false;

                const { detail, components } = await renderDetail(interaction, {
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

            if (pv.idx >= newTotal) pv.idx = Math.max(0, newTotal - 1);

            const embed = new EmbedBuilder()
                .setTitle(`🖼 写真管理: ${safeText(post.name || '(名称不明)', 200)}`)
                .setDescription(newTotal ? `写真 ${pv.idx + 1}/${newTotal}` : '写真はありません');

            if (newTotal && urls2[pv.idx]) {
                embed.setImage(urls2[pv.idx]);
            }

            return interaction.update({
                embeds: [embed],
                components: photoManagerComponents(guildId, ownerId, postId, newTotal),
            });
        }

        if (id.startsWith('mine:filter:')) {
            const [, , filter, gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            }
            if (userId !== ownerId) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
            }

            const st = mineState.get(k);
            if (!st) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: '一覧がありません' });
            }

            if (!['all', 'visited', 'planned'].includes(filter)) {
                return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'フィルタが不正です' });
            }

            st.visitFilter = filter;
            st.page = 0;
            mineState.set(k, st);

            return renderMineList(interaction, guildId, userId, { update: true });
        }

        // Mine list paging/back
        if (id.startsWith('mine:prev:') || id.startsWith('mine:next:')) {
            const [, dir, gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            const st = mineState.get(k);
            if (!st?.results?.length) return interaction.reply({ flags: MessageFlags.Ephemeral, content: '一覧がありません' });

            const ownPosts = Array.isArray(st.posts) ? st.posts : [];
            const postMap = new Map(ownPosts.map(p => [p.id, p]));
            const filteredIds = [];

            for (const pid of st.results) {
                const p = postMap.get(pid);
                if (!p) continue;
                if (!visitFilterMatch(st, p)) continue;
                filteredIds.push(pid);
            }

            if (!filteredIds.length) {
                return interaction.update({
                    embeds: [
                        new EmbedBuilder()
                            .setTitle('📚 自分の記録')
                            .setDescription('(まだありません)')
                    ],
                    components: homeComponents(),
                });
            }

            const pageSize = 9;
            const maxPage = Math.max(0, Math.ceil(filteredIds.length / pageSize) - 1);

            st.page = Number(st.page) || 0;
            st.page += dir === 'prev' ? -1 : 1;

            if (st.page < 0) st.page = 0;
            if (st.page > maxPage) st.page = maxPage;

            mineState.set(k, st);

            return renderMineList(interaction, guildId, userId, { update: true });
        }

        if (id.startsWith('mine:home:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            await interaction.update({ embeds: [homeEmbed()], components: homeComponents() });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        if (id.startsWith('mine:back:')) {
            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

            await renderMineList(interaction, guildId, userId, { update: true });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }

        // User select menu (search filter user)
        if (interaction.isUserSelectMenu()) {
            const id = interaction.customId;
            if (!id.startsWith('search:userPick:')) return;

            const [, , gid, ownerId] = id.split(':');
            if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

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
            if (id.startsWith('create:searchPlace:') || id.startsWith('edit:searchPlace:')) {
                const parts = id.split(':');
                const mode = parts[0];
                const gid = parts[2];
                const ownerId = parts[3];

                if (interaction.guildId !== gid) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
                }

                const d = draftRating.get(k);
                if (!d || d.mode !== mode) {
                    return interaction.reply({
                        flags: MessageFlags.Ephemeral,
                        content: mode === 'create' ? '新規登録状態がありません' : '編集状態がありません'
                    });
                }

                placeSearchState.set(k, {
                    mode,
                    query: d.name ?? '',
                    results: [],
                    page: 0,
                    nextPageToken: '',
                    loadingMore: false,
                });

                return interaction.showModal(
                    buildPlaceSearchModal(gid, ownerId, mode, d.name ?? '')
                );
            }

            if (id.startsWith('place:pick:')) {
                const [, , gid, ownerId] = id.split(':');

                if (interaction.guildId !== gid) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
                }

                const picked = interaction.values?.[0];
                if (!picked || picked === 'none') {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '候補を選択してください' });
                }

                const st = placeSearchState.get(k);
                if (!st) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'お店検索状態がありません' });
                }

                const idx = Number(picked);
                const selected = st.results?.[idx];

                if (!selected) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '候補が見つかりません' });
                }

                const d = draftRating.get(k);
                if (!d || d.mode !== st.mode) {
                    return interaction.reply({
                        flags: MessageFlags.Ephemeral,
                        content: st.mode === 'create' ? '新規登録状態がありません' : '編集状態がありません'
                    });
                }

                d.name = selected.name || d.name || '';
                d.mapUrl = selected.mapUrl || d.mapUrl || '';
                d.place = {
                    placeId: selected.placeId,
                    name: selected.name,
                    address: selected.address,
                    mapUrl: selected.mapUrl,
                };
                draftRating.set(k, d);
                placeSearchState.delete(k);

                if (st.mode === 'create') {
                    return renderCreatePanel(interaction, guildId, userId, { update: true });
                }

                return renderEditPanel(interaction, guildId, userId, { update: true });
            }

            // ===== 検索：都道府県ピック =====
            if (id.startsWith('search:prefPick:')) {
                const parts = id.split(':');
                const gid = parts[2];
                const ownerId = parts[3];
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const picked = interaction.values ?? [];
                const page = Number(parts[4] || 0);

                const st = searchState.get(k) ?? {
                    userIdFilter: null,
                    prefectureFilters: [],
                    tagFilters: [],
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
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
                }

                const picked = interaction.values?.[0] ?? '';
                const d = draftRating.get(k);

                if (!d || d.mode !== mode) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '途中状態がありません' });
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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const postId = interaction.values?.[0];
                if (!postId || postId === 'none') {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '選択が不正です' });
                }

                await ensureCacheLoadedForGuild(interaction.guild, userId);
                const post = await getPostByIdForViewer(postId, guildId, userId);
                if (!post) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'データが見つかりません' });
                }

                const { detail, components } = await renderDetail(interaction, {
                    post,
                    guildId,
                    userId,
                    fromMine: false,
                    total: searchState.get(k)?.results?.length || 1,
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
            return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
            if (userId !== ownerId) return interaction.reply({ ephemeral: true, content: 'これはあなたの操作ではありません' });

            const postId = interaction.values?.[0];
            if (!postId || postId === 'none') {
                return interaction.reply({ ephemeral: true, content: '選択が不正です' });
            }

            const st = mineState.get(k);
            const ownPosts = Array.isArray(st?.posts) ? st.posts : [];
            const postMap = new Map(ownPosts.map(p => [p.id, p]));
            const post = postMap.get(postId) ?? await getPostByIdForViewer(postId, guildId, userId);

            if (!post) {
                return interaction.update({
                    content: 'データが見つかりません',
                    embeds: [],
                    components: homeComponents(),
                });
            }

            const { detail, components } = await renderDetail(interaction, {
                post,
                guildId,
                userId,
                fromMine: true,
                total: 1,
                forceHomeBack: false,
            });

            await interaction.update({
                content: '',
                embeds: [detail],
                components,
            });
            await clearOtherUiMessages(interaction, guildId, userId, interaction.message.id);
            return;
        }   // ← interaction.isStringSelectMenu() を閉じる

        // Modal submit
        if (interaction.isModalSubmit()) {
            const id = interaction.customId;

            if (id.startsWith('modalPlaceSearch:')) {
                const [, gid, ownerId, mode] = id.split(':');

                if (interaction.guildId !== gid) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
                }

                const d = draftRating.get(k);
                if (!d || d.mode !== mode) {
                    return interaction.reply({
                        flags: MessageFlags.Ephemeral,
                        content: mode === 'create' ? '新規登録状態がありません' : '編集状態がありません'
                    });
                }

                const query = interaction.fields.getTextInputValue('placeQuery')?.trim() ?? '';
                if (!query) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '検索語を入力してください' });
                }

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

                try {
                    const first = await searchGooglePlacesText(query);

                    placeSearchState.set(k, {
                        mode,
                        query,
                        results: first.results ?? [],
                        page: 0,
                        nextPageToken: first.nextPageToken ?? '',
                        loadingMore: false,
                    });

                    const st = placeSearchState.get(k);

                    const ref = mode === 'create'
                        ? createPanelPromptRef.get(k)
                        : editPanelPromptRef.get(k);

                    const updated = await editPromptRef(ref, {
                        content: '',
                        embeds: [buildPlaceSearchEmbed(st)],
                        components: placeSearchComponents(guildId, userId, st),
                    });

                    if (updated) {
                        try { await interaction.deleteReply(); } catch { }
                        return;
                    }

                    await interaction.editReply({
                        content: '',
                        embeds: [buildPlaceSearchEmbed(st)],
                        components: placeSearchComponents(guildId, userId, st),
                    });

                    const sent = await interaction.fetchReply().catch(() => null);
                    if (sent?.id) {
                        addUiMessageId(guildId, userId, sent.id);
                    }

                    return;
                } catch (e) {
                    return interaction.editReply({
                        content: `店検索に失敗しました: ${e.message}`,
                        embeds: [],
                        components: [],
                    });
                }
            }

            if (id.startsWith('modalCreateComment:')) {
                const [, gid, ownerId] = id.split(':');
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'create') {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '新規登録状態がありません' });
                }

                const comment = interaction.fields.getTextInputValue('comment')?.trim() ?? '';

                if (comment.length > 500) {
                    return interaction.reply({
                        flags: MessageFlags.Ephemeral,
                        content: `コメントは500文字以内で入力してください（現在 ${comment.length}文字）`
                    });
                }

                d.comment = comment;
                draftRating.set(k, d);

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'create') {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '新規登録状態がありません' });
                }

                d.tags = parseTags(interaction.fields.getTextInputValue('tags'));
                draftRating.set(k, d);

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'create') {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '新規登録状態がありません' });
                }

                d.url = interaction.fields.getTextInputValue('url')?.trim() ?? '';
                draftRating.set(k, d);

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'create') {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '新規登録状態がありません' });
                }

                d.mapUrl = interaction.fields.getTextInputValue('mapUrl')?.trim() ?? '';
                draftRating.set(k, d);

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'create') {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '新規登録状態がありません' });
                }

                const name = interaction.fields.getTextInputValue('name')?.trim();
                if (!name) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'お店の名前は必須です' });
                }

                d.name = name;
                draftRating.set(k, d);

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'edit') {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '編集状態がありません' });
                }

                const name = interaction.fields.getTextInputValue('name')?.trim();
                if (!name) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'お店の名前は必須です' });
                }

                d.name = name;
                draftRating.set(k, d);

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'edit') {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '編集状態がありません' });
                }

                const comment = interaction.fields.getTextInputValue('comment')?.trim() ?? '';

                if (comment.length > 500) {
                    return interaction.reply({
                        flags: MessageFlags.Ephemeral,
                        content: `コメントは500文字以内で入力してください（現在 ${comment.length}文字）`
                    });
                }

                d.comment = comment;
                draftRating.set(k, d);

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'edit') {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '編集状態がありません' });
                }

                d.tags = parseTags(interaction.fields.getTextInputValue('tags'));
                draftRating.set(k, d);

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'edit') {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '編集状態がありません' });
                }

                d.url = interaction.fields.getTextInputValue('url')?.trim() ?? '';
                draftRating.set(k, d);

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const d = draftRating.get(k);
                if (!d || d.mode !== 'edit') {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '編集状態がありません' });
                }

                d.mapUrl = interaction.fields.getTextInputValue('mapUrl')?.trim() ?? '';
                draftRating.set(k, d);

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

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
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                }
                if (userId !== ownerId) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });
                }

                const d = draftRating.get(k);
                if (!d || d.mode !== mode) {
                    return interaction.reply({ flags: MessageFlags.Ephemeral, content: '途中状態がありません' });
                }

                const raw = interaction.fields.getTextInputValue('visitedDate');
                const normalized = normalizeVisitedDate(raw);

                if (normalized === null) {
                    return interaction.reply({
                        flags: MessageFlags.Ephemeral,
                        content: '行った日付は YYYY/MM/DD または YYYY-MM-DD 形式で入力してください',
                    });
                }

                d.visitedDate = normalized || '';
                draftRating.set(k, d);

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

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
                if (interaction.guildId !== gid) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'ギルド不一致です' });
                if (userId !== ownerId) return interaction.reply({ flags: MessageFlags.Ephemeral, content: 'これはあなたの操作ではありません' });

                const keyword = interaction.fields.getTextInputValue('keyword')?.trim() ?? '';

                const st = searchState.get(k) ?? {
                    userIdFilter: null,
                    prefectureFilters: [],
                    tagFilters: [],
                    keyword: '',
                    ratingFilters: [],
                    results: [],
                    page: 0
                };
                st.keyword = keyword;
                searchState.set(k, st);

                await interaction.deferReply({ flags: MessageFlags.Ephemeral });

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
        console.error('InteractionCreate error', {
            customId: interaction?.customId,
            type: interaction?.type,
            userId: interaction?.user?.id,
            guildId: interaction?.guildId,
            message: e?.message,
            stack: e?.stack,
        });

        if (interaction.isRepliable()) {
            try {
                const gid = interaction.guildId;
                const uid = interaction.user?.id;

                let errMsg;
                if (interaction.deferred || interaction.replied) {
                    errMsg = await interaction.followUp({
                        flags: MessageFlags.Ephemeral,
                        content: `エラー: ${e.message}`
                    });
                } else {
                    errMsg = await interaction.reply({
                        flags: MessageFlags.Ephemeral,
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
    let uploadingMsg = null;

    try {
        if (msg.author.bot) return;
        if (!msg.guildId) return;

        const k = keyOf(msg.guildId, msg.author.id);
        const wait = awaitingPhoto.get(k);
        if (!wait) return;

        if (wait.guildId !== msg.guildId) return;
        if (wait.channelId !== msg.channelId) return;

        const imgs = [...(msg.attachments?.values() ?? [])].filter(a => isImageAttachment(a));
        if (!imgs.length) return;

        if (wait.uiMessageRef) {
            await editPromptRef(wait.uiMessageRef, {
                content: '',
                embeds: [
                    new EmbedBuilder()
                        .setTitle('📷 写真を登録しています')
                        .setDescription('少し待ってください...')
                ],
                components: [],
            });
        }

        const addedImages = [];

        for (const img of imgs) {
            const res = await fetch(img.url);
            const buffer = Buffer.from(await res.arrayBuffer());

            const uploaded = await uploadPostImageToStorage({
                guildId: msg.guildId,
                postId: wait.postId,
                sourceBuffer: buffer,
                filename: img.name,
                contentType: img.contentType,
            });

            const { error: insertError } = await supabase
                .from('post_images')
                .insert({
                    post_id: wait.postId,
                    image_url: uploaded.url,
                    storage_path: uploaded.path,
                });

            if (insertError) {
                throw new Error(`post_images追加失敗: ${insertError.message}`);
            }
        }

        cacheReadyByGuild.set(msg.guildId, false);
        await ensureCacheLoadedForGuild(msg.guild, msg.author.id);

        const post = await getPostByIdForViewer(wait.postId, msg.guildId, msg.author.id);
        if (!post) {
            if (uploadingMsg) {
                try { await uploadingMsg.delete(); } catch { }
            }
            awaitingPhoto.delete(k);
            return;
        }

        if (wait.uiMessageRef) {
            if (wait.backTo === 'home') {
                openDetailAfterCreateState.set(k, { postId: post.id });

                await editPromptRef(wait.uiMessageRef, {
                    content: `✅ **${post.name}** に写真を追加しました`,
                    embeds: [
                        confirmEmbed('📄 詳細を開きますか？', `**${post.name}**`)
                    ],
                    components: openDetailAfterCreateComponents(
                        msg.guildId,
                        msg.author.id
                    ),
                });
            } else {
                const nav = getDetailNavState(msg.guildId, msg.author.id, post.id);
                const fromMine = nav?.fromMine ?? cameFromMine(k, post.id, mineState);
                const forceHomeBack = nav?.forceHomeBack ?? false;

                const member = msg.member ?? await msg.guild.members.fetch(msg.author.id).catch(() => null);

                const { detail, components } = await renderDetail(
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
        }

        awaitingPhoto.delete(k);

        try { await msg.delete(); } catch { }
    } catch (e) {
        console.error(e);

        if (uploadingMsg) {
            try { await uploadingMsg.delete(); } catch { }
        }
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
