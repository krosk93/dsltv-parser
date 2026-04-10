const fs = require('fs');
const path = require('path');
const turf = require('@turf/turf');
const Config = require('../core/Config');
const TextNormalizer = require('../utils/TextNormalizer');
const GeometryUtils = require('../utils/GeometryUtils');
const PDFParser = require('../io/PDFParser');
const AdifWfsClient = require('../clients/AdifWfsClient');
const StationService = require('./StationService');
const GeoDataService = require('./GeoDataService');
const StatsService = require('./StatsService');

const LINE_PK_TRANSFORMS = {
    // Line 230 (Picamoixons-Reus) uses a decreasing PK system (68.6 down to 0) in WFS,
    // while DSLTV documents use an increasing system inherited from Line 200.
    // Origin at Picamoixons is PK 68.61.
    '230': (pk) => 137.22 - pk
};

class LTVProcessor {
    constructor() {
        this.geoCache = {};
    }

    async convertToJson() {
        console.log('Migrating PDFs to JSONs...');
        const files = fs.readdirSync(Config.PDF_DIR)
            .filter(f => f.toLowerCase().endsWith('.pdf'))
            .map(f => ({ name: f, path: path.join(Config.PDF_DIR, f) }));

        for (const file of files) {
            console.log(`Parsing ${file.name}...`);
            const parsedData = await PDFParser.parse(file.path);
            const newFileName = file.name.replace('.pdf', '.json');
            const finalPath = path.join(Config.PDF_DIR, newFileName);
            fs.writeFileSync(finalPath, JSON.stringify(parsedData, null, 2));
            fs.unlinkSync(file.path);
        }
    }

    async processFilesBySuffix(suffix, outputPath) {
        const files = fs.readdirSync(Config.PDF_DIR)
            .filter(f => f.toLowerCase().endsWith(suffix))
            .map(f => {
                const dateMatch = f.match(/^(\d{4})(\d{2})(\d{2})/);
                return {
                    name: f,
                    path: path.join(Config.PDF_DIR, f),
                    date: dateMatch ? `${dateMatch[1]}-${dateMatch[2]}-${dateMatch[3]}` : '9999-99-99'
                };
            })
            .sort((a, b) => a.date.localeCompare(b.date));

        const globalLtvMap = new Map();
        let latestDate = null;
        
        for (const file of files) {
            latestDate = file.date;
            console.log(`Parsing ${file.name}...`);
            const records = JSON.parse(fs.readFileSync(file.path, 'utf8'));
            let migrationNeeded = false;
            for (const rec of records) {
                if (rec.code && /^\d{9}$/.test(rec.code)) {
                    rec.code = TextNormalizer.hashCode(rec.code);
                    migrationNeeded = true;
                }
            }
            if (migrationNeeded) {
                fs.writeFileSync(file.path, JSON.stringify(records, null, 2));
            }

            for (const rec of records) {
                if (!globalLtvMap.has(rec.code)) {
                    const { line, ...rest } = rec;
                    globalLtvMap.set(rec.code, {
                        ...rest,
                        line: line,
                        firstAppearanceDate: file.date,
                        lastSeen: file.date
                    });
                } else {
                    globalLtvMap.get(rec.code).lastSeen = file.date;
                }
            }
        }

        const grouped = {};
        for (const ltv of globalLtvMap.values()) {
            const line = ltv.line;
            const { line: _, ...ltvData } = ltv;
            if (!grouped[line]) grouped[line] = [];
            grouped[line].push(ltvData);
        }

        const sortedGrouped = {};
        Object.keys(grouped).sort().forEach(key => { sortedGrouped[key] = grouped[key]; });

        const isAv = suffix.includes('_dhltv');
        let enriched = await this.geocode(sortedGrouped, isAv);
        enriched = await this.reverseGeocode(enriched);

        fs.writeFileSync(outputPath, JSON.stringify(enriched, null, 2));
        
        const discoveredLines = await AdifWfsClient.discoverAllLines();
        const lineStats = await StatsService.generateLineStats(enriched, discoveredLines, isAv, latestDate);

        return { enriched, lineStats };
    }

    async geocode(ltvData, isAv = false) {
        console.log('Geocoding entries...');
        const designSpeeds = await AdifWfsClient.fetchDesignSpeeds();
        
        // Phase 1: WFS-based geocoding
        let wfsResolved = 0;
        let wfsFailed = 0;

        // Pre-group entries by line number for efficiency
        const groupedByLine = {};
        for (const lineName in ltvData) {
            const match = lineName.match(/LÍNEA\s+(\d+)/);
            if (match) {
                const ln = match[1];
                if (!groupedByLine[ln]) groupedByLine[ln] = [];
                groupedByLine[ln].push({ name: lineName, records: ltvData[lineName] });
            }
        }

        for (const lineNum in groupedByLine) {
            const allTramos = await AdifWfsClient.fetchLineTramos(lineNum);
            if (allTramos.length === 0) {
                wfsFailed += groupedByLine[lineNum].reduce((sum, g) => sum + g.records.length, 0);
                continue;
            }

            for (const group of groupedByLine[lineNum]) {
                for (const record of group.records) {
                    let startKm = parseFloat(record.startKm);
                    let endKm = parseFloat(record.endKm);
                    if (isNaN(startKm)) continue;

                    let currentStartKm = startKm;
                    let currentEndKm = endKm;
                    let pkTransformed = false;

                    const findCandidates = (s, e) => {
                        const minP = Math.min(s, isNaN(e) ? s : e);
                        const maxP = Math.max(s, isNaN(e) ? s : e);
                        const TOLERANCE = 1.0;
                        return allTramos.filter(t => {
                            const minT = Math.min(t.pki, t.pkd);
                            const maxT = Math.max(t.pki, t.pkd);
                            return (maxP >= minT - TOLERANCE) && (minP <= maxT + TOLERANCE);
                        });
                    };

                    let candidates = findCandidates(currentStartKm, currentEndKm);

                    if (candidates.length === 0 && LINE_PK_TRANSFORMS[lineNum]) {
                        currentStartKm = LINE_PK_TRANSFORMS[lineNum](startKm);
                        currentEndKm = isNaN(endKm) ? NaN : LINE_PK_TRANSFORMS[lineNum](endKm);
                        candidates = findCandidates(currentStartKm, currentEndKm);
                        if (candidates.length > 0) {
                            pkTransformed = true;
                        }
                    }

                    if (candidates.length === 0) {
                        wfsFailed++;
                        continue;
                    }

                    const minPk = Math.min(currentStartKm, isNaN(currentEndKm) ? currentStartKm : currentEndKm);
                    const maxPk = Math.max(currentStartKm, isNaN(currentEndKm) ? currentStartKm : currentEndKm);
                    const TOLERANCE = 1.0;

                    // Disambiguate parallel tracks
                    if (candidates.length > 1) {
                        const recordStations = TextNormalizer.splitStations(record.stations);
                        const foundCoords = StationService.getCoordinatesForStations(recordStations);

                        if (foundCoords.length > 0) {
                            const refCoords = {
                                lat: foundCoords.reduce((sum, c) => sum + c.lat, 0) / foundCoords.length,
                                lon: foundCoords.reduce((sum, c) => sum + c.lon, 0) / foundCoords.length
                            };

                            // Filter candidates by geographic proximity to stations to avoid wrong PK matches in distant areas
                            candidates = candidates.filter(t => {
                                const tramoMidPk = (t.pki + t.pkd) / 2;
                                const tramoCoords = GeometryUtils.findCoordsOnTramo(t, tramoMidPk);
                                if (!tramoCoords) return true; 
                                const dist = Math.sqrt(Math.pow(tramoCoords.lat - refCoords.lat, 2) + Math.pow(tramoCoords.lon - refCoords.lon, 2));
                                return dist < 0.3; // Approx 33km tolerance
                            });

                            if (candidates.length === 0) {
                                // Fallback: if filter was too aggressive, restore candidates
                                // (This shouldn't happen with 33km tolerance and 80km gap)
                                candidates = allTramos.filter(t => {
                                    const minT = Math.min(t.pki, t.pkd);
                                    const maxT = Math.max(t.pki, t.pkd);
                                    return (maxPk >= minT - TOLERANCE) && (minPk <= maxT + TOLERANCE);
                                });
                            }

                            let startCandidates = candidates.filter(t => {
                                const minT = Math.min(t.pki, t.pkd);
                                const maxT = Math.max(t.pki, t.pkd);
                                return (currentStartKm >= minT - TOLERANCE) && (currentStartKm <= maxT + TOLERANCE);
                            });

                            if (startCandidates.length > 1) {
                                let minDistance = Infinity;
                                let bestStart = startCandidates[0];
                                for (const cand of startCandidates) {
                                    const coords = GeometryUtils.findCoordsOnTramo(cand, currentStartKm);
                                    if (coords) {
                                        const dist = Math.sqrt(Math.pow(coords.lat - refCoords.lat, 2) + Math.pow(coords.lon - refCoords.lon, 2));
                                        if (dist < minDistance) {
                                            minDistance = dist;
                                            bestStart = cand;
                                        }
                                    }
                                }
                                const startPrefix = bestStart.codtramo.substring(0, 7);
                                candidates = candidates.filter(t => t.codtramo.startsWith(startPrefix));
                            }
                        }
                    }

                    candidates.sort((a, b) => {
                        const midA = (a.pki + a.pkd) / 2;
                        const midB = (b.pki + b.pkd) / 2;
                        return currentStartKm < currentEndKm ? midA - midB : midB - midA;
                    });

                    let totalPath = [];
                    let totalDelay = 0;
                    let minDesignSpeed = Infinity;

                    for (const tramo of candidates) {
                        const tramoPath = GeometryUtils.sliceTramoByPk(tramo, currentStartKm, isNaN(currentEndKm) ? currentStartKm : currentEndKm);
                        if (tramoPath.length > 0) {
                            if (totalPath.length > 0) {
                                const lastPoint = totalPath[totalPath.length - 1];
                                const firstPoint = tramoPath[0];
                                const jumpDist = Math.sqrt(Math.pow(lastPoint[0] - firstPoint[0], 2) + Math.pow(lastPoint[1] - firstPoint[1], 2));
                                if (jumpDist > 0.05) continue;
                                if (lastPoint[0] === firstPoint[0] && lastPoint[1] === firstPoint[1]) {
                                    totalPath.push(...tramoPath.slice(1));
                                } else {
                                    totalPath.push(...tramoPath);
                                }
                            } else {
                                totalPath.push(...tramoPath);
                            }

                            if (designSpeeds.has(tramo.codtramo)) {
                                const dSpeed = designSpeeds.get(tramo.codtramo);
                                minDesignSpeed = Math.min(minDesignSpeed, dSpeed);
                                const ltvSpeedMatch = record.speed.match(/(\d+)/);
                                if (ltvSpeedMatch && !isNaN(currentEndKm)) {
                                    const ltvSpeed = parseInt(ltvSpeedMatch[1], 10);
                                    const tMin = Math.min(tramo.pki, tramo.pkd);
                                    const tMax = Math.max(tramo.pki, tramo.pkd);
                                    const overlapMin = Math.max(minPk, tMin);
                                    const overlapMax = Math.min(maxPk, tMax);
                                    const dist = Math.max(0, overlapMax - overlapMin);
                                    if (dist > 0) {
                                        totalDelay += GeometryUtils.calculateEnhancedDelay(ltvSpeed, dSpeed, dist, isAv);
                                    }
                                }
                            }
                        }
                    }

                    if (totalPath.length > 0) {
                        record.path = totalPath;
                        const midIndex = Math.floor(totalPath.length / 2);
                        record.latitude = totalPath[midIndex][1];
                        record.longitude = totalPath[midIndex][0];
                        record.geocodingMethod = pkTransformed ? 'wfs_transformed' : 'wfs';
                        if (minDesignSpeed !== Infinity) {
                            record.designSpeed = minDesignSpeed;
                            record.delaySeconds = Math.round(totalDelay * 10) / 10;
                        }
                        wfsResolved++;
                    } else {
                        wfsFailed++;
                    }
                }
            }
        }

        // Phase 2: Station fallback
        console.log('  Phase 2: Station-based fallback...');
        let stationResolved = 0;
        for (const lineName in ltvData) {
            for (const record of ltvData[lineName]) {
                if (record.latitude && record.longitude) continue;
                const recordStations = TextNormalizer.splitStations(record.stations);
                const foundCoords = StationService.getCoordinatesForStations(recordStations);
                if (foundCoords.length > 0) {
                    record.latitude = foundCoords.reduce((sum, c) => sum + c.lat, 0) / foundCoords.length;
                    record.longitude = foundCoords.reduce((sum, c) => sum + c.lon, 0) / foundCoords.length;
                    record.geocodingMethod = 'station';
                    stationResolved++;
                }
            }
        }
        return ltvData;
    }

    async reverseGeocode(ltvData) {
        console.log('Reverse geocoding entries...');
        const cacheData = {};
        for (const lineName in ltvData) {
            for (const record of ltvData[lineName]) {
                if (record.latitude && record.longitude) {
                    const lat = record.latitude.toFixed(4);
                    const lon = record.longitude.toFixed(4);
                    const cacheKey = `${lat},${lon}`;

                    if (this.geoCache[cacheKey]) {
                        record.province = this.geoCache[cacheKey].province;
                        record.state = this.geoCache[cacheKey].state;
                        continue;
                    }

                    const { province, ccaa } = GeoDataService.getGeoInfoForPoint([record.longitude, record.latitude]);
                    if (province) record.province = province;
                    if (ccaa) record.state = ccaa;
                    this.geoCache[cacheKey] = { province, state: ccaa };
                }
            }
        }
        return ltvData;
    }

    async reprocess() {
        console.log('Reprocessing all JSONs...');
        await this.convertToJson();
        
        console.log('--- Processing DSLTV ---');
        const { enriched: ltv, lineStats: ltvStats } = await this.processFilesBySuffix('_dsltv.json', Config.LTV_JSON);
        
        console.log('--- Processing DHLTV ---');
        const { enriched: av, lineStats: avStats } = await this.processFilesBySuffix('_dhltv.json', Config.LTV_AV_JSON);
                const combinedStatsMap = new Map();
        [...ltvStats, ...avStats].forEach(s => {
            const lineNumMatch = s.line.match(/LÍNEA\s+(\d{3})/);
            const lineId = lineNumMatch ? lineNumMatch[1] : s.line;
            
            if (!combinedStatsMap.has(lineId)) {
                combinedStatsMap.set(lineId, JSON.parse(JSON.stringify(s)));
                const entry = combinedStatsMap.get(lineId);
                entry.networks = [s.network];
                delete entry.network;
            } else {
                const existing = combinedStatsMap.get(lineId);
                existing.ltvTotalKm = Math.round((existing.ltvTotalKm + s.ltvTotalKm) * 100) / 100;
                existing.ltvPercentage = existing.totalLengthKm > 0 ? Math.round((existing.ltvTotalKm / existing.totalLengthKm) * 10000) / 100 : 0;
                if (!existing.networks.includes(s.network)) existing.networks.push(s.network);
                
                // Efficiently merge geography
                s.geography.forEach(g => {
                    let eg = existing.geography.find(ex => ex.name === g.name);
                    if (!eg) {
                        existing.geography.push(JSON.parse(JSON.stringify(g)));
                    } else {
                        eg.totalKm = Math.max(eg.totalKm, g.totalKm);
                        eg.ltvKm = Math.round((eg.ltvKm + g.ltvKm) * 100) / 100;
                        eg.ltvPercentage = eg.totalKm > 0 ? Math.round((eg.ltvKm / eg.totalKm) * 10000) / 100 : 0;
                        
                        // Merge provinces
                        g.provinces.forEach(p => {
                            let ep = eg.provinces.find(exP => exP.name === p.name);
                            if (!ep) {
                                eg.provinces.push(JSON.parse(JSON.stringify(p)));
                            } else {
                                ep.totalKm = Math.max(ep.totalKm, p.totalKm);
                                ep.ltvKm = Math.round((ep.ltvKm + p.ltvKm) * 100) / 100;
                                ep.ltvPercentage = ep.totalKm > 0 ? Math.round((ep.ltvKm / ep.totalKm) * 10000) / 100 : 0;
                            }
                        });
                    }
                });
            }
        });

        // Re-apply provincial merge logic correctly
        combinedStatsMap.forEach((s, lineId) => {
            const hasHistoryInAV = avStats.some(av => av.line.includes(lineId) && av.ltvTotalKm > 0);
            const isInHighSpeedRange = Config.HIGH_SPEED_RANGE.includes(lineId);
            s.networks = (hasHistoryInAV || (isInHighSpeedRange && !ltvStats.some(c => c.line.includes(lineId) && c.ltvTotalKm > 0))) 
                ? ['high_speed'] : ['conventional'];
        });

        fs.writeFileSync(Config.LINES_JSON, JSON.stringify(Array.from(combinedStatsMap.values()).sort((a,b) => a.line.localeCompare(b.line)), null, 2));
        console.log('Lines statistics generated.');
        return { ltv, av };
    }
}

module.exports = LTVProcessor;
