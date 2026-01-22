import { readIndex } from '../../../utils/indexManager.js';
import { getDatabase } from '../../../utils/databaseAdapter.js';

const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
};

export async function onRequest(context) {
    const { request, env } = context;

    try {
        if (request.method === 'OPTIONS') {
            return new Response(null, { status: 204, headers: corsHeaders });
        }

        const url = new URL(request.url);
        const action = url.searchParams.get('action');

        switch (action) {
            case 'backup':
                return await handleBackup(context);
            case 'restore':
                return await handleRestoreAsync(context);
            case 'restore-status':
                return await handleRestoreStatus(context);
            default:
                return new Response(JSON.stringify({ error: '不支持的操作' }), {
                    status: 400,
                    headers: { 'Content-Type': 'application/json', ...corsHeaders }
                });
        }
    } catch (error) {
        console.error('备份操作错误:', error);
        return new Response(JSON.stringify({ error: '操作失败: ' + error.message }), {
            status: 500,
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
    }
}

// 处理备份操作
async function handleBackup(context) {
    const { env } = context;
    try {
        const db = getDatabase(env);

        const backupData = {
            timestamp: Date.now(),
            version: '2.2.5',
            data: {
                fileCount: 0,
                files: {},
                settings: {}
            }
        };

        // 首先从索引中读取所有文件信�?
        const indexResult = await readIndex(context, {
            count: -1,  // 获取所有文�?
            start: 0,
            includeSubdirFiles: true  // 包含子目录下的文�?
        });
        backupData.data.fileCount = indexResult.files.length;

        // 备份文件数据
        for (const file of indexResult.files) {
            const fileId = file.id;
            const metadata = file.metadata;

            // 对于TelegramNew渠道且IsChunked为true的文件，需要从数据库读取其�?
            if (metadata.Channel === 'TelegramNew' && metadata.IsChunked === true) {
                try {
                    const fileData = await db.getWithMetadata(fileId);
                    backupData.data.files[fileId] = {
                        metadata: metadata,
                        value: fileData.value
                    };
                } catch (error) {
                    console.error(`读取分块文件 ${fileId} 失败:`, error);
                    // 如果读取失败，仍然保存元数据
                    backupData.data.files[fileId] = {
                        metadata: metadata,
                        value: null
                    };
                }
            } else {
                // 其他文件直接保存索引中的元数�?
                backupData.data.files[fileId] = {
                    metadata: metadata,
                    value: null
                };
            }
        }

        // 备份系统设置
        const settingsList = await db.list({ prefix: 'manage@' });
        for (const key of settingsList.keys) {
            // 忽略索引文件
            if (key.name.startsWith('manage@index')) continue;

            const setting = await db.get(key.name);
            if (setting) {
                backupData.data.settings[key.name] = setting;
            }
        }

        const backupJson = JSON.stringify(backupData, null, 2);

        return new Response(backupJson, {
            status: 200,
            headers: {
                'Content-Type': 'application/json',
                'Content-Disposition': `attachment; filename="imgbed_backup_${new Date().toISOString().split('T')[0]}.json"`,
                ...corsHeaders
            }
        });
    } catch (error) {
        throw new Error('备份失败: ' + error.message);
    }
}

// 处理恢复操作
async function parseBackupPayload(request) {
    const contentType = request.headers.get('content-type') || '';

    if (contentType.includes('application/json')) {
        return await request.json();
    }

    if (contentType.includes('multipart/form-data')) {
        const formData = await request.formData();
        let file = null;

        for (const value of formData.values()) {
            if (value && typeof value === 'object' && typeof value.text === 'function') {
                file = value;
                break;
            }
        }

        if (!file) {
            throw new Error('backup file not found in form data');
        }

        const text = await file.text();
        return JSON.parse(text);
    }

    if (contentType.includes('text/plain')) {
        const text = await request.text();
        return JSON.parse(text);
    }

    throw new Error('unsupported content-type: ' + contentType);
}

const RESTORE_TASK_PREFIX = 'manage@sysConfig@restoreTask@'
const RESTORE_DATA_PREFIX = 'manage@sysConfig@restoreData@'
const RESTORE_PROGRESS_INTERVAL = 20
const RESTORE_CHUNK_SIZE = 20

function buildRestoreTaskKey(taskId) {
    return `${RESTORE_TASK_PREFIX}${taskId}`
}

function buildRestoreDataKey(taskId, type, chunkIndex) {
    return `${RESTORE_DATA_PREFIX}${taskId}@${type}@${chunkIndex}`
}

function calcRestoreProgress(task) {
    const total = (task.totalFiles || 0) + (task.totalSettings || 0)
    const done = (task.restoredFiles || 0) + (task.restoredSettings || 0) + (task.failedFiles || 0) + (task.failedSettings || 0)
    return total > 0 ? Math.floor((done / total) * 100) : 100
}

function createRestoreTask(taskId, totals, backupTimestamp) {
    return {
        id: taskId,
        status: 'queued',
        message: 'queued',
        createdAt: Date.now(),
        updatedAt: Date.now(),
        progress: 0,
        totalFiles: totals.totalFiles || 0,
        totalSettings: totals.totalSettings || 0,
        fileChunks: totals.fileChunks || 0,
        settingChunks: totals.settingChunks || 0,
        restoredFiles: 0,
        restoredSettings: 0,
        failedFiles: 0,
        failedSettings: 0,
        lastError: null,
        backupTimestamp: backupTimestamp || null,
        cursor: {
            phase: 'files',
            chunkIndex: 0,
            index: 0
        }
    }
}

function normalizeRestoreCursor(task) {
    if (!task.cursor) {
        task.cursor = { phase: 'files', chunkIndex: 0, index: 0 }
        return
    }
    if (!task.cursor.phase) task.cursor.phase = 'files'
    if (typeof task.cursor.chunkIndex !== 'number') task.cursor.chunkIndex = 0
    if (typeof task.cursor.index !== 'number') task.cursor.index = 0
}

async function saveRestoreTask(db, taskKey, task) {
    task.updatedAt = Date.now()
    task.progress = calcRestoreProgress(task)
    await db.put(taskKey, JSON.stringify(task))
}

async function loadRestoreTask(db, taskKey) {
    const raw = await db.get(taskKey)
    return raw ? JSON.parse(raw) : null
}

function chunkEntries(entries, size) {
    const chunks = []
    for (let i = 0; i < entries.length; i += size) {
        chunks.push(entries.slice(i, i + size))
    }
    return chunks
}

async function storeRestoreData(db, taskId, backupData) {
    const fileEntries = Object.entries(backupData.data?.files || {})
    const settingEntries = Object.entries(backupData.data?.settings || {})

    const fileChunks = chunkEntries(fileEntries, RESTORE_CHUNK_SIZE)
    const settingChunks = chunkEntries(settingEntries, RESTORE_CHUNK_SIZE)

    for (let i = 0; i < fileChunks.length; i++) {
        await db.put(buildRestoreDataKey(taskId, 'files', i), JSON.stringify(fileChunks[i]))
    }

    for (let i = 0; i < settingChunks.length; i++) {
        await db.put(buildRestoreDataKey(taskId, 'settings', i), JSON.stringify(settingChunks[i]))
    }

    return {
        totalFiles: fileEntries.length,
        totalSettings: settingEntries.length,
        fileChunks: fileChunks.length,
        settingChunks: settingChunks.length
    }
}

async function loadRestoreChunk(db, taskId, type, chunkIndex) {
    const raw = await db.get(buildRestoreDataKey(taskId, type, chunkIndex))
    return raw ? JSON.parse(raw) : []
}

async function deleteRestoreChunks(db, task) {
    if (task.fileChunks) {
        for (let i = 0; i < task.fileChunks; i++) {
            await db.delete(buildRestoreDataKey(task.id, 'files', i))
        }
    }
    if (task.settingChunks) {
        for (let i = 0; i < task.settingChunks; i++) {
            await db.delete(buildRestoreDataKey(task.id, 'settings', i))
        }
    }
}

async function restoreFileEntry(db, key, fileData, task) {
    try {
        if (fileData?.value) {
            await db.put(key, fileData.value, { metadata: fileData.metadata })
        } else if (fileData?.metadata) {
            await db.put(key, '', { metadata: fileData.metadata })
        }
        task.restoredFiles++
    } catch (error) {
        task.failedFiles++
        task.lastError = error.message
        console.error(`Restore file ${key} failed:`, error)
    }
}

async function restoreSettingEntry(db, key, value, task) {
    try {
        await db.put(key, value)
        task.restoredSettings++
    } catch (error) {
        task.failedSettings++
        task.lastError = error.message
        console.error(`Restore setting ${key} failed:`, error)
    }
}

async function advanceRestoreTask(db, taskKey, task) {
    normalizeRestoreCursor(task)

    if (task.status !== 'running') {
        task.status = 'running'
        task.message = 'running'
    }

    if (!task.fileChunks && !task.settingChunks) {
        await saveRestoreTask(db, taskKey, task)
        return task
    }

    let processed = 0

    while (processed < RESTORE_PROGRESS_INTERVAL) {
        if (task.cursor.phase === 'files') {
            if (!task.fileChunks || task.cursor.chunkIndex >= task.fileChunks) {
                task.cursor.phase = 'settings'
                task.cursor.chunkIndex = 0
                task.cursor.index = 0
                continue
            }

            const chunk = await loadRestoreChunk(db, task.id, 'files', task.cursor.chunkIndex)
            while (task.cursor.index < chunk.length && processed < RESTORE_PROGRESS_INTERVAL) {
                const entry = chunk[task.cursor.index] || []
                const key = entry[0]
                const fileData = entry[1]
                if (key) {
                    await restoreFileEntry(db, key, fileData, task)
                }
                task.cursor.index++
                processed++
            }

            if (task.cursor.index >= chunk.length) {
                task.cursor.chunkIndex++
                task.cursor.index = 0
            }

            if (processed >= RESTORE_PROGRESS_INTERVAL) {
                break
            }
            continue
        }

        if (task.cursor.phase === 'settings') {
            if (!task.settingChunks || task.cursor.chunkIndex >= task.settingChunks) {
                break
            }

            const chunk = await loadRestoreChunk(db, task.id, 'settings', task.cursor.chunkIndex)
            while (task.cursor.index < chunk.length && processed < RESTORE_PROGRESS_INTERVAL) {
                const entry = chunk[task.cursor.index] || []
                const key = entry[0]
                const value = entry[1]
                if (key) {
                    await restoreSettingEntry(db, key, value, task)
                }
                task.cursor.index++
                processed++
            }

            if (task.cursor.index >= chunk.length) {
                task.cursor.chunkIndex++
                task.cursor.index = 0
            }

            if (processed >= RESTORE_PROGRESS_INTERVAL) {
                break
            }
            continue
        }

        task.cursor.phase = 'settings'
        task.cursor.chunkIndex = 0
        task.cursor.index = 0
    }

    const filesDone = (task.restoredFiles + task.failedFiles) >= (task.totalFiles || 0)
    const settingsDone = (task.restoredSettings + task.failedSettings) >= (task.totalSettings || 0)

    if (filesDone && settingsDone) {
        if (task.failedFiles > 0 || task.failedSettings > 0) {
            task.status = 'completed_with_errors'
            task.message = 'completed with errors'
        } else {
            task.status = 'completed'
            task.message = 'completed'
        }
        await saveRestoreTask(db, taskKey, task)
        await deleteRestoreChunks(db, task)
        return task
    }

    await saveRestoreTask(db, taskKey, task)
    return task
}

async function handleRestoreStatus(context) {
    const { request, env } = context
    const url = new URL(request.url)
    const taskId = url.searchParams.get('taskId')

    if (!taskId) {
        return new Response(JSON.stringify({ error: 'taskId is required' }), {
            status: 400,
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        })
    }

    const db = getDatabase(env)
    const taskKey = buildRestoreTaskKey(taskId)
    let task = await loadRestoreTask(db, taskKey)

    if (!task) {
        return new Response(JSON.stringify({ error: 'task not found' }), {
            status: 404,
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        })
    }

    if (task.status === 'queued' || task.status === 'running') {
        task = await advanceRestoreTask(db, taskKey, task)
    }

    return new Response(JSON.stringify({ success: true, task }), {
        status: 200,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
    })
}

async function handleRestoreAsync(context) {
    const { request, env } = context

    try {
        let backupData
        try {
            backupData = await parseBackupPayload(request)
        } catch (parseError) {
            return new Response(JSON.stringify({ error: 'Invalid backup JSON: ' + parseError.message }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            })
        }

        if (!backupData.data || !backupData.data.files || !backupData.data.settings) {
            return new Response(JSON.stringify({ error: 'Invalid backup schema' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            })
        }

        const taskId = `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`
        const taskKey = buildRestoreTaskKey(taskId)
        const db = getDatabase(env)

        let totals
        try {
            totals = await storeRestoreData(db, taskId, backupData)
        } catch (storeError) {
            return new Response(JSON.stringify({ error: 'Failed to store restore data: ' + storeError.message }), {
                status: 500,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            })
        }

        const task = createRestoreTask(taskId, totals, backupData.timestamp)
        await saveRestoreTask(db, taskKey, task)

        return new Response(JSON.stringify({
            success: true,
            message: 'restore started',
            taskId,
            stats: {
                totalFiles: task.totalFiles,
                totalSettings: task.totalSettings
            }
        }), {
            status: 202,
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        })
    } catch (error) {
        throw new Error('Restore failed: ' + error.message)
    }
}