const fs = require('fs');
const path = require('path');
const { WFS_CACHE_DIR } = require('../core/Config');

class CacheManager {
    constructor(cacheDir = WFS_CACHE_DIR) {
        this.cacheDir = cacheDir;
        this.ensureCacheDir();
    }

    ensureCacheDir() {
        if (!fs.existsSync(this.cacheDir)) {
            fs.mkdirSync(this.cacheDir, { recursive: true });
        }
    }

    getFilePath(key) {
        // Ensure the key is a safe filename
        const safeKey = key.replace(/[^a-z0-9_.-]/gi, '_');
        return path.join(this.cacheDir, `${safeKey}.json`);
    }

    get(key) {
        const filePath = this.getFilePath(key);
        if (fs.existsSync(filePath)) {
            try {
                return JSON.parse(fs.readFileSync(filePath, 'utf8'));
            } catch (err) {
                console.warn(`Error reading cache for ${key}:`, err.message);
                return null;
            }
        }
        return null;
    }

    set(key, data) {
        const filePath = this.getFilePath(key);
        try {
            fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
            return true;
        } catch (err) {
            console.error(`Error writing cache for ${key}:`, err.message);
            return false;
        }
    }

    exists(key) {
        return fs.existsSync(this.getFilePath(key));
    }
}

module.exports = new CacheManager();
