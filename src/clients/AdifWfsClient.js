const axios = require('axios');
const CacheManager = require('../io/CacheManager');
const GeoDataService = require('../services/GeoDataService');
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
        let tramos = this.cache.get(cacheKey);

        if (tramos) {
            // Check if cached data already has geography enrichment
            if (tramos.length > 0 && !tramos[0].geography) {
                console.log(`  Enriching cached tramos for line ${lineNum}...`);
                tramos.forEach(t => GeoDataService.enrichTramoWithGeo(t));
                this.cache.set(cacheKey, tramos);
            }
            return tramos;
        }

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

            // Enrich before caching
            if (tramos.length > 0) {
                tramos.forEach(t => GeoDataService.enrichTramoWithGeo(t));
            }

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
        const speeds = {};
        const PAGE_SIZE = 5000;
        let startIndex = 0;
        let hasMore = true;

        try {
            while (hasMore) {
                const url = `${DESIGN_SPEED_WFS}?service=WFS&version=2.0.0&request=GetFeature&typename=tn-ra:DesignSpeed&count=${PAGE_SIZE}&startIndex=${startIndex}`;
                const response = await axios.get(url, { timeout: 60000 });
                const xml = response.data;

                const regex = /<tn-ra:DesignSpeed[^>]+gml:id=\"TN_DesignSpeed_(.*?)\">.*?<tn-ra:speed[^>]*>(.*?)<\/tn-ra:speed>/gs;
                let match;
                let countFound = 0;
                while ((match = regex.exec(xml)) !== null) {
                    speeds[match[1]] = parseFloat(match[2]);
                    countFound++;
                }

                console.log(`    Fetched page starting at ${startIndex}: ${countFound} entries`);
                if (countFound < PAGE_SIZE) {
                    hasMore = false;
                } else {
                    startIndex += PAGE_SIZE;
                }
            }

            this.cache.set(cacheKey, speeds);
            console.log(`  Total design speed entries loaded: ${Object.keys(speeds).length}`);
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
        if (this.discoveredLinesCache) return this.discoveredLinesCache;
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
            this.discoveredLinesCache = Array.from(lineSet).sort();
            return this.discoveredLinesCache;
        } catch (err) {
            console.warn(`    Line discovery failed: ${err.message}.`);
            return [];
        }
    }
}

module.exports = new AdifWfsClient();
