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
                return await handleRestore(request, env);
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

async function handleRestore(request, env) {
    try {
        const db = getDatabase(env);

        let backupData;
        try {
            backupData = await parseBackupPayload(request);
        } catch (parseError) {
            return new Response(JSON.stringify({ error: 'Invalid backup JSON: ' + parseError.message }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        // 验证备份文件格式
        if (!backupData.data || !backupData.data.files || !backupData.data.settings) {
            return new Response(JSON.stringify({ error: '备份文件格式无效' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json', ...corsHeaders }
            });
        }

        let restoredFiles = 0;
        let restoredSettings = 0;

        // 恢复文件数据
        for (const [key, fileData] of Object.entries(backupData.data.files)) {
            try {
                if (fileData.value) {
                    // 对于有value的文件（如telegram分块文件），恢复完整数据
                    await db.put(key, fileData.value, {
                        metadata: fileData.metadata
                    });
                } else if (fileData.metadata) {
                    // 只恢复元数据
                    await db.put(key, '', {
                        metadata: fileData.metadata
                    });
                }
                restoredFiles++;
            } catch (error) {
                console.error(`恢复文件 ${key} 失败:`, error);
            }
        }

        // 恢复系统设置
        for (const [key, value] of Object.entries(backupData.data.settings)) {
            try {
                await db.put(key, value);
                restoredSettings++;
            } catch (error) {
                console.error(`恢复设置 ${key} 失败:`, error);
            }
        }

        return new Response(JSON.stringify({
            success: true,
            message: '恢复完成',
            stats: {
                restoredFiles,
                restoredSettings,
                backupTimestamp: backupData.timestamp
            }
        }), {
            status: 200,
            headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
    } catch (error) {
        throw new Error('恢复失败: ' + error.message);
    }
}
