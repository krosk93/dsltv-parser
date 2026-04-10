const axios = require('axios');
const CacheManager = require('../io/CacheManager');
const { WFS_BASE, DESIGN_SPEED_WFS } = require('../core/Config');

class AdifWfsClient {
    constructor() {
        this.cache = CacheManager;
        // Inject GeoDataService later to avoid circular dependency if needed, 
        // or just handle it in the orchestrator.
    }

    /**
     * Fetches all TramosServicio for a given line number with full geometry.
     */
    async fetchLineTramos(lineNum) {
        const cacheKey = `tramos_${lineNum}`;
        const cached = this.cache.get(cacheKey);
        if (cached) return cached;

        try {
            const url = `${WFS_BASE}?service=WFS&version=2.0.0&request=GetFeature` +
                `&typeName=Tramificacion:TramosServicio&outputFormat=application/json` +
                `&srsName=EPSG:4326` +
                `&CQL_FILTER=cod_linea LIKE '${lineNum}%25'`;
            
            const response = await axios.get(url, { timeout: 30000 });
            const data = response.data;
            if (!data.features || data.features.length === 0) return [];

            const tramos = data.features
                .filter(f => {
                    const cl = f.properties.cod_linea;
                    if (!cl) return false;
                    const match = cl.match(/^(\d+)/);
                    return match && match[1] === lineNum;
                })
                .map(f => ({
                    codtramo: f.properties.codtramo,
                    pki: f.properties.pki,
                    pkd: f.properties.pkd,
                    geometry: f.geometry,
                    orden: f.properties.orden
                }));

            this.cache.set(cacheKey, tramos);
            return tramos;
        } catch (err) {
            console.warn(`  WFS fetchLineTramos failed for line ${lineNum}: ${err.message}`);
            return [];
        }
    }

    /**
     * Fetches all PKTeoricos for a given codtramo prefix.
     */
    async fetchPKTeoricos(codtramoPrefix) {
        const cacheKey = `pks_${codtramoPrefix}`;
        const cached = this.cache.get(cacheKey);
        if (cached) return cached;

        try {
            const url = `${WFS_BASE}?service=WFS&version=2.0.0&request=GetFeature` +
                `&typeName=Tramificacion:PKTeoricos&outputFormat=application/json` +
                `&srsName=EPSG:4326` +
                `&CQL_FILTER=codtramo LIKE '${codtramoPrefix}%25'`;
            
            const response = await axios.get(url, { timeout: 30000 });
            const data = response.data;
            if (!data.features || data.features.length === 0) return [];

            const result = data.features
                .filter(f => f.geometry && f.properties.pk != null)
                .map(f => ({
                    pk: f.properties.pk,
                    lat: f.geometry.coordinates[1],
                    lon: f.geometry.coordinates[0],
                    codtramo: f.properties.codtramo
                }))
                .sort((a, b) => a.pk - b.pk);

            this.cache.set(cacheKey, result);
            return result;
        } catch (err) {
            console.warn(`  WFS PKTeoricos query failed for prefix ${codtramoPrefix}: ${err.message}`);
            return [];
        }
    }

    /**
     * Fetches all design speeds from ADIF's secondary WFS (INSPIRE).
     */
    async fetchDesignSpeeds() {
        const cacheKey = 'design_speeds';
        const cached = this.cache.get(cacheKey);
        if (cached) return new Map(Object.entries(cached));

        console.log('  Fetching track design speeds from ADIF INSPIRE WFS...');
        try {
            const url = `${DESIGN_SPEED_WFS}?service=WFS&version=2.0.0&request=GetFeature&typename=tn-ra:DesignSpeed`;
            const response = await axios.get(url, { timeout: 60000 });
            const xml = response.data;
            const speeds = {};

            const regex = /<tn-ra:DesignSpeed[^>]+gml:id=\"TN_DesignSpeed_(.*?)\">.*?<tn-ra:speed[^>]*>(.*?)<\/tn-ra:speed>/gs;
            let match;
            while ((match = regex.exec(xml)) !== null) {
                speeds[match[1]] = parseFloat(match[2]);
            }

            this.cache.set(cacheKey, speeds);
            console.log(`  Loaded ${Object.keys(speeds).length} design speed entries`);
            return new Map(Object.entries(speeds));
        } catch (err) {
            console.warn(`  Track design speed query failed: ${err.message}`);
            return new Map();
        }
    }

    /**
     * Discovers all line numbers in the ADIF network.
     */
    async discoverAllLines() {
        console.log('  Discovering all ADIF lines for comprehensive statistics...');
        try {
            const url = `${WFS_BASE}?service=WFS&version=2.0.0&request=GetFeature&typeName=Tramificacion:TramosServicio&outputFormat=application/json&propertyName=cod_linea`;
            const response = await axios.get(url, { timeout: 30000 });
            const lineSet = new Set();
            response.data.features.forEach(f => {
                const cl = f.properties.cod_linea;
                if (cl) {
                    const m = cl.match(/^(\d{3})/);
                    if (m) lineSet.add(m[1]);
                }
            });
            return Array.from(lineSet).sort();
        } catch (err) {
            console.warn(`    Line discovery failed: ${err.message}.`);
            return [];
        }
    }
}

module.exports = new AdifWfsClient();
