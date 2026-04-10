const crypto = require('crypto');

class TextNormalizer {
    static normalize(text) {
        if (!text) return '';
        return text.toString()
            .toLowerCase()
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .replace(/'/g, '')
            .replace(/\b(madrid|barcelona|valencia|sevilla|donostia|bilbao|barna|irun)[\s.-]+/g, '')
            .replace(/\b(apd|cgd|estacion|estacio|est|mercan|pk|p\.k\.|bif|vif)[\s.-]+/g, '')
            .replace(/glories(?:[\s.-]*clot)?/g, 'glorias')
            .replace(/[^a-z0-9]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }

    static clean(val) {
        if (!val) return '';
        return val.toString().replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
    }

    static formatDateTime(date, time) {
        const d = this.clean(date);
        const t = this.clean(time);
        if (!d && !t) return '';
        if (!t) return d;
        if (!d) return t;
        return `${d} ${t}`;
    }

    static hashCode(code) {
        if (!code) return '';
        const codeStr = code.toString().trim();
        // SHA256 in base64 is 44 characters and ends with '='.
        if (codeStr.length === 44 && codeStr.endsWith('=') && !/^\d+$/.test(codeStr)) {
            return codeStr;
        }
        return crypto.createHash('sha256').update(codeStr).digest('base64');
    }

    /**
     * Splits stations string using the regex: [\w\d\.]-\s
     * Ensures the character before the hyphen is preserved.
     */
    static splitStations(stationsStr) {
        if (!stationsStr) return [];
        return stationsStr
            .replace(/([\w\d\.])-\s/g, '$1|')
            .split('|')
            .map(s => s.trim())
            .filter(s => s.length > 0);
    }
}

module.exports = TextNormalizer;
