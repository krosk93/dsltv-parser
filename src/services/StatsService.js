const AdifWfsClient = require('../clients/AdifWfsClient');
const GeoDataService = require('./GeoDataService');

class StatsService {
    async generateLineStats(enrichedData, discoveredLines, isHighSpeed, latestDate) {
        const lineStats = [];

        for (const lineNum of discoveredLines) {
            const tramos = await AdifWfsClient.fetchLineTramos(lineNum);
            if (tramos.length === 0) continue;

            const lineName = `LÍNEA ${lineNum}`;
            const enrichedMatchingNames = Object.keys(enrichedData).filter(k => k.startsWith(`LÍNEA ${lineNum}`));
            const allRelevantEnrichedData = [];
            enrichedMatchingNames.forEach(k => allRelevantEnrichedData.push(...enrichedData[k]));

            // 1. Merge LTV intervals for the total line (ONLY for active LTVs)
            const intervals = [];
            for (const ltv of allRelevantEnrichedData) {
                if (ltv.lastSeen !== latestDate) continue;
                const start = parseFloat(ltv.startKm);
                const end = parseFloat(ltv.endKm);
                if (!isNaN(start) && !isNaN(end)) {
                    intervals.push([Math.min(start, end), Math.max(start, end)]);
                }
            }
            intervals.sort((a, b) => a[0] - b[0]);
            let mergedIntervals = [];
            if (intervals.length > 0) {
                let [pS, pE] = intervals[0];
                for (let i = 1; i < intervals.length; i++) {
                    let [cS, cE] = intervals[i];
                    if (cS <= pE) pE = Math.max(pE, cE);
                    else { mergedIntervals.push([pS, pE]); [pS, pE] = [cS, cE]; }
                }
                mergedIntervals.push([pS, pE]);
            }

            const lineGeoStats = new Map(); // "CCAA|Province" -> { totalKm: 0, ltvKm: 0, rawIntervals: [] }
            
            // 2. Spatial analysis of the full line geometry (per province/ccaa)
            for (const t of tramos) {
                if (t.geography) {
                    for (const geo of t.geography) {
                        const key = `${geo.ccaa}|${geo.province}`;
                        if (!lineGeoStats.has(key)) lineGeoStats.set(key, { totalKm: 0, ltvKm: 0, rawIntervals: [] });
                        lineGeoStats.get(key).rawIntervals.push([Math.min(geo.startPk, geo.endPk), Math.max(geo.startPk, geo.endPk)]);
                    }
                }
            }

            // Merge Raw Intervals to avoid overcounting overlapping segments
            for (const [key, stats] of lineGeoStats) {
                if (stats.rawIntervals.length > 0) {
                    stats.rawIntervals.sort((a, b) => a[0] - b[0]);
                    let merged = [];
                    let [pS, pE] = stats.rawIntervals[0];
                    for (let i = 1; i < stats.rawIntervals.length; i++) {
                        let [cS, cE] = stats.rawIntervals[i];
                        if (cS <= pE) pE = Math.max(pE, cE);
                        else { merged.push([pS, pE]); [pS, pE] = [cS, cE]; }
                    }
                    merged.push([pS, pE]);
                    stats.totalKm = merged.reduce((sum, [s, e]) => sum + (e - s), 0);
                }
            }

            // 3. Spatial analysis of LTV paths (per province/ccaa)
            if (mergedIntervals.length > 0) {
                for (const [sKm, eKm] of mergedIntervals) {
                    const sMin = Math.min(sKm, eKm);
                    const sMax = Math.max(sKm, eKm);
                    for (const [key, stats] of lineGeoStats) {
                        if (stats.rawIntervals.length > 0) {
                            // stats.rawIntervals already contains sorted merged segments? 
                            // No, let's re-run the merge logic or use the one above.
                            // Actually, I just calculated stats.totalKm using a local 'merged' array.
                            // Let's redo it properly.
                            const mergedTrack = [];
                            let [pS, pE] = stats.rawIntervals[0];
                            for (let i = 1; i < stats.rawIntervals.length; i++) {
                                let [cS, cE] = stats.rawIntervals[i];
                                if (cS <= pE) pE = Math.max(pE, cE);
                                else { mergedTrack.push([pS, pE]); [pS, pE] = [cS, cE]; }
                            }
                            mergedTrack.push([pS, pE]);

                            for (const [mS, mE] of mergedTrack) {
                                const iMin = Math.max(sMin, mS);
                                const iMax = Math.min(sMax, mE);
                                const overlap = Math.max(0, iMax - iMin);
                                stats.ltvKm += overlap;
                            }
                        }
                    }
                }
            }

            // 4. Format into nested structure
            const ccaaGrouped = new Map();
            let totalLtvKm = 0;
            let lineTotalKm = 0;

            for (const [key, stat] of lineGeoStats) {
                const [ccaa, province] = key.split('|');
                if (!ccaaGrouped.has(ccaa)) ccaaGrouped.set(ccaa, { totalKm: 0, ltvKm: 0, provinces: new Map() });
                const cg = ccaaGrouped.get(ccaa);
                cg.totalKm += stat.totalKm;
                cg.ltvKm += stat.ltvKm;
                cg.provinces.set(province, { name: province, totalKm: stat.totalKm, ltvKm: stat.ltvKm });
                totalLtvKm += stat.ltvKm;
                lineTotalKm += stat.totalKm;
            }

            const formattedBreakdown = Array.from(ccaaGrouped.entries()).map(([ccaa, data]) => ({
                name: ccaa,
                totalKm: Math.round(data.totalKm * 100) / 100,
                ltvKm: Math.round(data.ltvKm * 100) / 100,
                ltvPercentage: data.totalKm > 0 ? Math.round((data.ltvKm / data.totalKm) * 10000) / 100 : 0,
                provinces: Array.from(data.provinces.values()).map(p => ({
                    name: p.name,
                    totalKm: Math.round(p.totalKm * 100) / 100,
                    ltvKm: Math.round(p.ltvKm * 100) / 100,
                    ltvPercentage: p.totalKm > 0 ? Math.round((p.ltvKm / p.totalKm) * 10000) / 100 : 0
                }))
            })).sort((a, b) => b.totalKm - a.totalKm);

            lineStats.push({
                line: lineName,
                network: isHighSpeed ? 'high_speed' : 'conventional',
                totalLengthKm: Math.round(lineTotalKm * 100) / 100,
                ltvTotalKm: Math.round(totalLtvKm * 100) / 100,
                ltvPercentage: lineTotalKm > 0 ? Math.round((totalLtvKm / lineTotalKm) * 10000) / 100 : 0,
                geography: formattedBreakdown
            });
        }

        return lineStats;
    }
}

module.exports = new StatsService();
