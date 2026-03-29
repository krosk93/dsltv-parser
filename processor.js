const fs = require('fs');
const path = require('path');
const { PDFParse } = require('pdf-parse');
const turf = require('@turf/turf');
const axios = require('axios');
const crypto = require('crypto');

const PDF_DIR = path.join(__dirname, 'pdfs');
const OUTPUT_DIR = path.join(__dirname, 'output');
const STATIONS_FILE = path.join(__dirname, 'stations.json');
const LTV_JSON = path.join(OUTPUT_DIR, 'ltv.json');

const WFS_BASE = 'https://ideadif.adif.es/gservices/Tramificacion/wfs';
const DESIGN_SPEED_WFS = 'https://ideadif.adif.es/services/wfs';
const WFS_CACHE_DIR = path.join(__dirname, 'wfs');

if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR);
if (!fs.existsSync(WFS_CACHE_DIR)) fs.mkdirSync(WFS_CACHE_DIR);

function normalize(text) {
    if (!text) return '';
    return text.toString()
        .toLowerCase()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/'/g, '')
        .replace(/\b(madrid|barcelona|valencia|sevilla|donostia|bilbao|barna|irun)[\s.-]+/g, '')
        .replace(/\b(apd|cgd|estacion|estacio|est|mercan|pk|p\.k\.|bif)[\s.-]+/g, '')
        .replace(/glories(?:[\s.-]*clot)?/g, 'glorias')
        .replace(/[^a-z0-9]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function clean(val) {
    if (!val) return '';
    return val.toString().replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
}

function formatDateTime(date, time) {
    const d = clean(date);
    const t = clean(time);
    if (!d && !t) return '';
    if (!t) return d;
    if (!d) return t;
    return `${d} ${t}`;
}

function hashCode(code) {
    if (!code) return '';
    const codeStr = code.toString().trim();
    // SHA256 in base64 is 44 characters and ends with '='.
    // LTV codes are 9-digit numbers like "000123456".
    if (codeStr.length === 44 && codeStr.endsWith('=') && !/^\d+$/.test(codeStr)) {
        return codeStr;
    }
    return crypto.createHash('sha256').update(codeStr).digest('base64');
}

async function extractVigorDate(filePath) {
    const dataBuffer = fs.readFileSync(filePath);
    const parser = new PDFParse({ data: dataBuffer });
    try {
        const result = await parser.getText({ first: 2 });
        const match = result.text.match(/(\d{2})\/(\d{2})\/(\d{4})\s(\d{2})\:(\d{2}).*Fecha Vigor/i);
        if (match) {
            return `${match[3]}${match[2]}${match[1]}_${match[4]}${match[5]}`; // YYYYMMDD_HHMM
        }
    } catch (err) {
        console.error('Error extracting date:', err);
    } finally {
        await parser.destroy();
    }
    return null;
}

async function isWeekly(filePath) {
    const dataBuffer = fs.readFileSync(filePath);
    const parser = new PDFParse({ data: dataBuffer });
    try {
        const result = await parser.getText({ partial: [1] });
        return result.text.includes('SEMANAL');
    } catch (err) {
        console.error('Error extracting date:', err);
    } finally {
        await parser.destroy();
    }
    return null;
}

async function parseSinglePdf(filePath) {
    const dataBuffer = fs.readFileSync(filePath);
    const parser = new PDFParse({ data: dataBuffer });
    const records = [];
    let currentLine = 'Unknown Line';

    try {
        const info = await parser.getInfo();
        const totalPages = info.total;
        const isWeekly = (await parser.getText({ partial: [1] })).text.includes('SEMANAL');


        for (let i = 1; i <= totalPages; i++) {
            let tableRows = [];
            try {
                const tableResult = await parser.getTable({ partial: [i] });
                if (tableResult.pages && tableResult.pages[0] && tableResult.pages[0].tables && tableResult.pages[0].tables.length > 0) {
                    tableRows = tableResult.pages[0].tables[0];
                } else {
                    tableRows = [];
                }
            } catch (err) {
                continue;
            }

            for (const row of tableRows) {
                if (row.includes('(CÓDIGO LTV)') || row.includes('Trayecto / Estación') || row.includes('Km.Ini')) continue;
                if (row.length >= 4 && row[0] === 'Fecha' && row[1] === 'Hora') continue;

                const fullRowText = row.join(' ').trim();
                if (fullRowText.startsWith('LÍNEA')) {
                    currentLine = fullRowText.replace(/\s+/g, ' ').trim();
                    continue;
                }

                const codeMatch = row[0] ? row[0].trim().match(/^\((\d{9})\)$/) : null;
                if (codeMatch) {
                    records.push({
                        line: currentLine,
                        code: hashCode(codeMatch[1]),
                        stations: clean(row[1]),
                        track: clean(row[2]),
                        startKm: clean(row[3]),
                        endKm: clean(row[4]),
                        speed: clean(row[5]),
                        reason: clean(row[6]),
                        startDateTime: formatDateTime(row[7], row[8]),
                        endDateTime: formatDateTime(row[9], row[10]),
                        schedule: clean(row[11]),
                        csv: clean(row[14]) === 'X',
                        comment: clean(row[15])
                    });
                }
            }
        }
    } finally {
        await parser.destroy();
    }
    return records;
}

/**
 * Extracts a single LineString from a geometry (LineString or MultiLineString).
 */
function extractLineString(geometry) {
    if (!geometry) return null;
    if (geometry.type === 'LineString') return geometry;
    if (geometry.type === 'MultiLineString') {
        const coords = [];
        for (const line of geometry.coordinates) {
            coords.push(...line);
        }
        return { type: 'LineString', coordinates: coords };
    }
    return null;
}

let provincesGeoJSON = null;
let ccaaGeoJSON = null;

async function ensureGeoDataLoaded() {
    if (provincesGeoJSON && ccaaGeoJSON) return;
    try {
        provincesGeoJSON = JSON.parse(fs.readFileSync(path.join(__dirname, 'provinces.geojson'), 'utf8'));
        ccaaGeoJSON = JSON.parse(fs.readFileSync(path.join(__dirname, 'communities.geojson'), 'utf8'));
    } catch (e) {
        console.warn('GeoJSON data not found');
    }
}

function enrichTramoWithGeo(tramo) {
    const line = extractLineString(tramo.geometry);
    if (!line) return;

    const coords = line.coordinates;
    const geoIntervals = [];
    if (coords.length < 2) return;

    const pStartKm = tramo.pki;
    const pEndKm = tramo.pkd;
    const totalDist = turf.length(line);

    let lastKey = null;
    let segmentStartKm = pStartKm;

    for (let i = 0; i < coords.length - 1; i++) {
        const p1 = coords[i];
        const p2 = coords[i+1];
        const mid = [ (p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2 ];
        const pointMid = turf.point(mid);
        const ratio = i / (coords.length - 1);
        const currentPk = pStartKm + (pEndKm - pStartKm) * ratio;

        let province = 'Unknown';
        let ccaa = 'Unknown';

        for (const f of provincesGeoJSON.features) {
            if (turf.booleanPointInPolygon(pointMid, f.geometry)) {
                province = f.properties.name;
                break;
            }
        }
        for (const f of ccaaGeoJSON.features) {
            if (turf.booleanPointInPolygon(pointMid, f.geometry)) {
                ccaa = f.properties.name;
                break;
            }
        }

        const key = `${ccaa}|${province}`;
        if (lastKey !== null && key !== lastKey) {
            geoIntervals.push({
                startPk: Math.min(segmentStartKm, currentPk),
                endPk: Math.max(segmentStartKm, currentPk),
                ccaa: lastKey.split('|')[0],
                province: lastKey.split('|')[1]
            });
            segmentStartKm = currentPk;
        }
        lastKey = key;
    }
    geoIntervals.push({
        startPk: Math.min(segmentStartKm, pEndKm),
        endPk: Math.max(segmentStartKm, pEndKm),
        ccaa: lastKey.split('|')[0],
        province: lastKey.split('|')[1]
    });
    tramo.geography = geoIntervals;
}

/**
 * Finds coordinates for a target PK on a specific tramo geometry.
 */
function findCoordsOnTramo(tramo, targetPk) {
    const line = extractLineString(tramo.geometry);
    if (!line) return null;

    // Calculate length if not already cached
    const length = turf.length(line);

    const range = tramo.pkd - tramo.pki;
    if (Math.abs(range) < 0.0001) {
        return { lon: line.coordinates[0][0], lat: line.coordinates[0][1] };
    }

    let ratio = (targetPk - tramo.pki) / range;
    ratio = Math.max(0, Math.min(1, ratio));

    const point = turf.along(line, ratio * length);
    return {
        lon: point.geometry.coordinates[0],
        lat: point.geometry.coordinates[1]
    };
}

/**
 * Slices a tramo geometry between two PKs.
 */
function sliceTramoByPk(tramo, startPk, endPk) {
    const line = extractLineString(tramo.geometry);
    if (!line) return [];

    const length = turf.length(line);
    const range = tramo.pkd - tramo.pki;
    if (Math.abs(range) < 0.0001) return [[line.coordinates[0][0], line.coordinates[0][1]]];

    const p1 = Math.max(0, Math.min(1, (startPk - tramo.pki) / range)) * length;
    const p2 = Math.max(0, Math.min(1, (endPk - tramo.pki) / range)) * length;

    const startDist = Math.min(p1, p2);
    const endDist = Math.max(p1, p2);

    try {
        const sliced = turf.lineSliceAlong(line, startDist, endDist);
        let coords = sliced.geometry.coordinates;
        // Reverse if needed to match startPk -> endPk direction
        if (p1 > p2) coords.reverse();
        return coords;
    } catch (e) {
        return [[line.coordinates[0][0], line.coordinates[0][1]]];
    }
}

/**
 * Fetches all TramosServicio for a given line number with full geometry.
 */
async function fetchLineTramos(lineNum) {
    const cacheFile = path.join(WFS_CACHE_DIR, `tramos_${lineNum}.json`);

    if (fs.existsSync(cacheFile)) {
        const tramos = JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
        // If cached tramos don't have geography labeling, enrich and update cache
        if (tramos.length > 0 && !tramos[0].geography) {
            await ensureGeoDataLoaded();
            if (provincesGeoJSON && ccaaGeoJSON) {
                console.log(`  Enriching cache for line ${lineNum} with geography...`);
                tramos.forEach(t => enrichTramoWithGeo(t));
                fs.writeFileSync(cacheFile, JSON.stringify(tramos, null, 2));
            }
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

        // Exact filter: ensures '210' doesn't match '2100'
        // Matches lineNum strictly followed by a non-digit or end of string
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

        fs.writeFileSync(cacheFile, JSON.stringify(tramos, null, 2));
        
        // Enrich with geography if not already present
        if (tramos.length > 0) {
            await ensureGeoDataLoaded();
            if (provincesGeoJSON && ccaaGeoJSON) {
                tramos.forEach(t => enrichTramoWithGeo(t));
                fs.writeFileSync(cacheFile, JSON.stringify(tramos, null, 2));
            }
        }
        
        return tramos;
    } catch (err) {
        console.warn(`  WFS fetchLineTramos failed for line ${lineNum}: ${err.message}`);
        return [];
    }
}

/**
 * Fetches all PKTeoricos (kilometric points with coordinates) for a given codtramo prefix.
 * Returns an array of { pk, lat, lon } sorted by pk.
 */
async function fetchPKTeoricos(codtramoPrefix) {
    const cacheFile = path.join(WFS_CACHE_DIR, `pks_${codtramoPrefix}.json`);

    // Check cache
    if (fs.existsSync(cacheFile)) {
        return JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
    }

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

        // Save to cache
        fs.writeFileSync(cacheFile, JSON.stringify(result, null, 2));

        return result;
    } catch (err) {
        console.warn(`  WFS PKTeoricos query failed for prefix ${codtramoPrefix}: ${err.message}`);
        return [];
    }
}

/**
 * Fetches all design speeds from ADIF's secondary WFS (INSPIRE).
 * Returns a Map of codtramo -> design speed (km/h).
 * Results are cached in a local file.
 */
async function fetchDesignSpeeds() {
    const cacheFile = path.join(WFS_CACHE_DIR, 'design_speeds.json');
    if (fs.existsSync(cacheFile)) {
        const data = JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
        return new Map(Object.entries(data));
    }

    console.log('  Fetching track design speeds from ADIF INSPIRE WFS...');
    try {
        const url = `${DESIGN_SPEED_WFS}?service=WFS&version=2.0.0&request=GetFeature&typename=tn-ra:DesignSpeed`;
        const response = await axios.get(url, { timeout: 60000 });
        const xml = response.data;
        const speeds = {};

        // Use regex for simple GML parsing (faster and no extra deps)
        const regex = /<tn-ra:DesignSpeed[^>]+gml:id=\"TN_DesignSpeed_(.*?)\">.*?<tn-ra:speed[^>]*>(.*?)<\/tn-ra:speed>/gs;
        let match;
        while ((match = regex.exec(xml)) !== null) {
            speeds[match[1]] = parseFloat(match[2]);
        }

        fs.writeFileSync(cacheFile, JSON.stringify(speeds, null, 2));
        console.log(`  Loaded ${Object.keys(speeds).length} design speed entries`);
        return new Map(Object.entries(speeds));
    } catch (err) {
        console.warn(`  Track design speed query failed: ${err.message}`);
        return new Map();
    }
}

/**
 * Calculates the extra time (delay) in seconds for a speed limitation,
 * including deceleration and acceleration phases.
 */
function calculateEnhancedDelay(ltvSpeedKmh, designSpeedKmh, distanceKm, isAv = false) {
    if (ltvSpeedKmh >= designSpeedKmh || ltvSpeedKmh <= 0) return 0;

    const Vd = designSpeedKmh / 3.6; // m/s
    const Vl = ltvSpeedKmh / 3.6;   // m/s
    const distanceM = distanceKm * 1000;

    // 1. Constant speed phase delay
    const delayConstant = distanceM * (1 / Vl - 1 / Vd);

    // 2. Deceleration phase delay
    let aDec = 1.0;
    if (isAv) {
        // v^2 = u^2 + 2ad -> 0 = (320/3.6)^2 - 2 * a * 8000 -> a = (320/3.6)^2 / 16000
        aDec = Math.pow(320 / 3.6, 2) / 16000;
    }
    const delayDec = Math.pow(Vd - Vl, 2) / (2 * aDec * Vd);

    // 3. Acceleration phase delay
    let delayAcc = 0;
    if (isAv) {
        // a = v / t -> (320 / 3.6) / 450
        const aAcc = (320 / 3.6) / 450;
        delayAcc = Math.pow(Vd - Vl, 2) / (2 * aAcc * Vd);
    } else {
        const accTable = [
            { v: 0, a: 0.8 },
            { v: 60, a: 0.75 },
            { v: 100, a: 0.6 },
            { v: 120, a: 0.5 },
            { v: 160, a: 0.4 },
            { v: 250, a: 0.25 },
            { v: 400, a: 0.1 }
        ];

        function getAccForV(vKmh) {
            for (let i = 0; i < accTable.length - 1; i++) {
                if (vKmh >= accTable[i].v && vKmh <= accTable[i + 1].v) {
                    const t = (vKmh - accTable[i].v) / (accTable[i + 1].v - accTable[i].v);
                    return accTable[i].a + t * (accTable[i + 1].a - accTable[i].a);
                }
            }
            return accTable[accTable.length - 1].a;
        }

        const step = 0.5; // km/h
        for (let v = ltvSpeedKmh; v < designSpeedKmh; v += step) {
            const vMs = v / 3.6;
            const a = getAccForV(v);
            delayAcc += ((step / 3.6) / a) * (1 - vMs / Vd);
        }
    }

    return Math.round((delayConstant + delayDec + delayAcc) * 10) / 10;
}

/**
 * Main geocoding function. Uses a two-phase approach:
 * 1. WFS-based geocoding using ADIF's PKTeoricos for precise railway coordinates
 * 2. Station-based fallback for entries that WFS couldn't resolve
 */
async function geocode(ltvData, isAv = false) {
    console.log('Geocoding entries...');

    const designSpeeds = await fetchDesignSpeeds();

    // Pre-load station map for disambiguation in Phase 1 and fallback in Phase 2
    const stationMap = new Map();
    if (fs.existsSync(STATIONS_FILE)) {
        const stationsData = JSON.parse(fs.readFileSync(STATIONS_FILE, 'utf8'));
        for (const s of stationsData) {
            if (s.DESCRIPCION && s.LATITUD && s.LONGITUD) {
                const norm = normalize(s.DESCRIPCION);
                if (norm.length > 0 && !stationMap.has(norm)) {
                    stationMap.set(norm, { lat: s.LATITUD, lon: s.LONGITUD });
                }
            }
        }
    }
    const stationList = Array.from(stationMap.entries()).map(([norm, coords]) => ({ norm, coords })).sort((a, b) => a.norm.length - b.norm.length);

    // ── Phase 1: WFS-based geocoding using ADIF PKTeoricos ──
    let wfsResolved = 0;
    let wfsFailed = 0;
    const lineTramosCache = new Map(); // lineNum → { tramos: Array, totalLength: number }

    try {
        console.log('  Phase 1: Querying ADIF WFS for precise railway coordinates...');

        // Collect unique line numbers
        const lineNumbers = new Set();
        for (const lineName in ltvData) {
            const match = lineName.match(/LÍNEA\s+(\d+)/);
            if (match) lineNumbers.add(match[1]);
        }
        console.log(`  Found ${lineNumbers.size} unique line numbers`);

        // For each line, resolve tramos with geometry
        for (const lineNum of lineNumbers) {
            const tramos = await fetchLineTramos(lineNum);
            if (tramos.length > 0) {
                // Calculate line length as the sum of geometry lengths, 
                // but avoid overcounting parallel tracks by using PK range coverage
                const pkIntervals = tramos.map(t => [t.pki, t.pkd]);
                const routeLength = calculateIntervalSum(pkIntervals);
                
                lineTramosCache.set(lineNum, { tramos, totalLength: routeLength });
                console.log(`  Line ${lineNum}: loaded ${tramos.length} segments, route length: ${routeLength.toFixed(2)} km`);
            }
        }

        // Apply WFS coordinates to each record
        for (const lineName in ltvData) {
            const match = lineName.match(/LÍNEA\s+(\d+)/);
            if (!match) continue;
            const lineNum = match[1];
            const lineInfo = lineTramosCache.get(lineNum);
            if (!lineInfo) continue;
            const allTramos = lineInfo.tramos;
            const lineTotalLength = lineInfo.totalLength;

            for (const record of ltvData[lineName]) {
                const startKm = parseFloat(record.startKm);
                const endKm = parseFloat(record.endKm);
                if (isNaN(startKm)) continue;

                const minPk = Math.min(startKm, isNaN(endKm) ? startKm : endKm);
                const maxPk = Math.max(startKm, isNaN(endKm) ? startKm : endKm);

                // PK Tolerance: allow matching tramos within 1km gap
                const TOLERANCE = 1.0;

                // Identify ALL tramos that overlap the PK range
                let candidates = allTramos.filter(t => {
                    const minT = Math.min(t.pki, t.pkd);
                    const maxT = Math.max(t.pki, t.pkd);
                    return (maxPk >= minT - TOLERANCE) && (minPk <= maxT + TOLERANCE);
                });

                if (candidates.length === 0) {
                    wfsFailed++;
                    continue;
                }

                // Disambiguate parallel tracks if needed (using station names)
                // Filter candidates to stay on the same "track" (codtramo suffix often helps)
                if (candidates.length > 1) {
                    const recordStations = record.stations.split(/[-\s]+/).filter(s => s.length > 2);
                    let refCoords = null;
                    for (const st of recordStations) {
                        const normSt = normalize(st);
                        if (stationMap.has(normSt)) {
                            refCoords = stationMap.get(normSt);
                            break;
                        }
                    }

                    if (refCoords) {
                        // Filter candidates that are reasonably close to the reference stations
                        // or just pick the best start tramo and follow its pattern
                        let startCandidates = candidates.filter(t => {
                            const minT = Math.min(t.pki, t.pkd);
                            const maxT = Math.max(t.pki, t.pkd);
                            return (startKm >= minT - TOLERANCE) && (startKm <= maxT + TOLERANCE);
                        });

                        if (startCandidates.length > 1) {
                            let minDistance = Infinity;
                            let bestStart = startCandidates[0];
                            for (const cand of startCandidates) {
                                const coords = findCoordsOnTramo(cand, startKm);
                                if (coords) {
                                    const dist = Math.sqrt(Math.pow(coords.lat - refCoords.lat, 2) + Math.pow(coords.lon - refCoords.lon, 2));
                                    if (dist < minDistance) {
                                        minDistance = dist;
                                        bestStart = cand;
                                    }
                                }
                            }
                            // Keep only candidates that match the start tram's track (suffix/codtramo)
                            const startPrefix = bestStart.codtramo.substring(0, 7);
                            candidates = candidates.filter(t => t.codtramo.startsWith(startPrefix));
                        }
                    }
                }

                // Sort candidates in order from startKm to endKm
                candidates.sort((a, b) => {
                    const midA = (a.pki + a.pkd) / 2;
                    const midB = (b.pki + b.pkd) / 2;
                    return startKm < endKm ? midA - midB : midB - midA;
                });

                let totalPath = [];
                let totalDelay = 0;
                let minDesignSpeed = Infinity;

                for (const tramo of candidates) {
                    const tramoPath = sliceTramoByPk(tramo, startKm, isNaN(endKm) ? startKm : endKm);
                    if (tramoPath.length > 0) {
                        // Spatial continuity check: avoid jumps > 5km
                        if (totalPath.length > 0) {
                            const lastPoint = totalPath[totalPath.length - 1];
                            const firstPoint = tramoPath[0];
                            const jumpDist = Math.sqrt(Math.pow(lastPoint[0] - firstPoint[0], 2) + Math.pow(lastPoint[1] - firstPoint[1], 2));
                            if (jumpDist > 0.05) { // ~5km
                                // If the LTV is very short, we might have picked multiple overlapping 
                                // but unrelated segments. Only keep the first one that matched our target.
                                continue;
                            }
                        }

                        // Check for duplicates at the junction
                        if (totalPath.length > 0) {
                            const lastPoint = totalPath[totalPath.length - 1];
                            const firstPoint = tramoPath[0];
                            if (lastPoint[0] === firstPoint[0] && lastPoint[1] === firstPoint[1]) {
                                totalPath.push(...tramoPath.slice(1));
                            } else {
                                totalPath.push(...tramoPath);
                            }
                        } else {
                            totalPath.push(...tramoPath);
                        }

                        // Accumulate delay if design speed is available
                        if (designSpeeds.has(tramo.codtramo)) {
                            const dSpeed = designSpeeds.get(tramo.codtramo);
                            minDesignSpeed = Math.min(minDesignSpeed, dSpeed);
                            const ltvSpeedMatch = record.speed.match(/(\d+)/);
                            if (ltvSpeedMatch && !isNaN(endKm)) {
                                const ltvSpeed = parseInt(ltvSpeedMatch[1], 10);
                                const tMin = Math.min(tramo.pki, tramo.pkd);
                                const tMax = Math.max(tramo.pki, tramo.pkd);
                                const overlapMin = Math.max(minPk, tMin);
                                const overlapMax = Math.min(maxPk, tMax);
                                const dist = Math.max(0, overlapMax - overlapMin);
                                if (dist > 0) {
                                    totalDelay += calculateEnhancedDelay(ltvSpeed, dSpeed, dist, isAv);
                                }
                            }
                        }
                    }
                }

                if (totalPath.length > 0) {
                    record.path = totalPath;
                    // For latitude/longitude, use the midpoint of the joined path
                    const midIndex = Math.floor(totalPath.length / 2);
                    record.latitude = totalPath[midIndex][1];
                    record.longitude = totalPath[midIndex][0];

                    record.geocodingMethod = 'wfs';
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
        console.log(`  WFS geocoding: ${wfsResolved} resolved, ${wfsFailed} unresolved`);
    } catch (err) {
        console.warn(`  WFS geocoding phase failed: ${err.message}`);
    }

    // ── Phase 2: Station-based fallback for unresolved entries ──
    console.log('  Phase 2: Station-based fallback for remaining entries...');
    let stationResolved = 0;
    for (const lineName in ltvData) {
        for (const record of ltvData[lineName]) {
            if (record.latitude && record.longitude) continue;

            const parts = record.stations.split(/-\s+|\s+-/);
            const coords = [];
            for (const part of parts) {
                const normPart = normalize(part);
                if (normPart.length === 0) continue;
                if (stationMap.has(normPart)) {
                    coords.push(stationMap.get(normPart));
                } else {
                    const alts = [normPart, normPart.replace(/^v/, 'b'), normPart.replace(/^b/, 'v'), normPart.replace('errenteria', 'renteria')];
                    let foundAlt = false;
                    for (const alt of alts) {
                        if (stationMap.has(alt)) {
                            coords.push(stationMap.get(alt));
                            foundAlt = true;
                            break;
                        }
                    }
                    if (!foundAlt) {
                        const match = stationList.find(s => s.norm.includes(normPart) || normPart.includes(s.norm));
                        if (match) coords.push(match.coords);
                    }
                }
            }
            if (coords.length > 0) {
                const avgLat = coords.reduce((sum, c) => sum + c.lat, 0) / coords.length;
                const avgLon = coords.reduce((sum, c) => sum + c.lon, 0) / coords.length;
                record.latitude = avgLat;
                record.longitude = avgLon;
                record.geocodingMethod = 'station';
                stationResolved++;
            }
        }
    }
    console.log(`  Station fallback: ${stationResolved} additional entries resolved`);
    return { enriched: ltvData, lineTramosCache };
}

async function reverseGeocode(ltvData) {
    console.log('Reverse geocoding entries with local GeoJSON data...');

    let provincesGeoJSON;
    let ccaaGeoJSON;

    try {
        provincesGeoJSON = JSON.parse(fs.readFileSync(path.join(__dirname, 'provinces.geojson'), 'utf8'));
        ccaaGeoJSON = JSON.parse(fs.readFileSync(path.join(__dirname, 'communities.geojson'), 'utf8'));
    } catch (e) {
        console.warn('GeoJSON data not found, skipping reverse geocoding');
        return ltvData;
    }

    const cacheData = {};

    for (const lineName in ltvData) {
        for (const record of ltvData[lineName]) {
            if (record.latitude && record.longitude) {
                const lat = record.latitude.toFixed(4);
                const lon = record.longitude.toFixed(4);
                const cacheKey = `${lat},${lon}`;

                if (cacheData[cacheKey]) {
                    if (cacheData[cacheKey].province) record.province = cacheData[cacheKey].province;
                    if (cacheData[cacheKey].state) record.state = cacheData[cacheKey].state;
                    continue;
                }

                try {
                    const point = turf.point([record.longitude, record.latitude]);
                    let province = '';
                    let state = '';

                    for (const feature of provincesGeoJSON.features) {
                        if (turf.booleanPointInPolygon(point, feature.geometry)) {
                            province = feature.properties.name;
                            break;
                        }
                    }

                    for (const feature of ccaaGeoJSON.features) {
                        if (turf.booleanPointInPolygon(point, feature.geometry)) {
                            state = feature.properties.name;
                            break;
                        }
                    }

                    if (province) record.province = province;
                    if (state) record.state = state;

                    cacheData[cacheKey] = { province, state };
                } catch (err) {
                    console.error(`Error reverse geocoding for ${lat}, ${lon}:`, err.message);
                }
            }
        }
    }
    return ltvData;
}

async function convertToJson() {
    console.log('Migrating PDFs to JSONs...');
    const files = fs.readdirSync(PDF_DIR)
        .filter(f => f.toLowerCase().endsWith('.pdf'))
        .map(f => {
            return {
                name: f,
                path: path.join(PDF_DIR, f)
            };
        });
    for (const file of files) {
        console.log(`Parsing ${file.name}...`);
        const parsedData = await parseSinglePdf(file.path);
        const newFileName = file.name.replace('.pdf', '.json');
        const finalPath = path.join(PDF_DIR, newFileName);
        fs.writeFileSync(finalPath, JSON.stringify(parsedData, null, 2));
        fs.unlinkSync(file.path);
    }
}

async function processFilesBySuffix(suffix, outputPath) {
    const files = fs.readdirSync(PDF_DIR)
        .filter(f => f.toLowerCase().endsWith(suffix))
        .map(f => {
            const dateMatch = f.match(/^(\d{4})(\d{2})(\d{2})/);
            return {
                name: f,
                path: path.join(PDF_DIR, f),
                date: dateMatch ? `${dateMatch[1]}-${dateMatch[2]}-${dateMatch[3]}` : '9999-99-99'
            };
        })
        .sort((a, b) => a.date.localeCompare(b.date));

    const globalLtvMap = new Map();
    let latestDate = null;
    
    for (const file of files) {
        latestDate = file.date; // Files are sorted by date ascending
        console.log(`Parsing ${file.name}...`);
        const records = JSON.parse(fs.readFileSync(file.path, 'utf8'));
        let migrationNeeded = false;
        for (let i = 0; i < records.length; i++) {
            const rec = records[i];
            // Check if code is in the old (9 digits) format
            if (rec.code && /^\d{9}$/.test(rec.code)) {
                rec.code = hashCode(rec.code);
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

    let { enriched, lineTramosCache } = await geocode(sortedGrouped, suffix.includes('_dhltv'));
    enriched = await reverseGeocode(enriched);

    fs.writeFileSync(outputPath, JSON.stringify(enriched, null, 2));
    console.log(`Regeneration complete for ${suffix}.`);

    // 0. Line Discovery: Fetch all line numbers to ensure we have stats for everything
    console.log('  Discovering all ADIF lines for comprehensive statistics...');
    let allAdifLines = [];
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
        allAdifLines = Array.from(lineSet).sort();
        console.log(`    Discovered ${allAdifLines.length} lines in ADIF network.`);
    } catch (e) {
        console.warn(`    Line discovery failed: ${e.message}. Using lines with LTVs only.`);
        allAdifLines = Object.keys(enriched).map(ln => ln.match(/LÍNEA\s+(\d{3})/)?.[1]).filter(Boolean);
    }

    // Load GeoJSON for geographical breakdown
    await ensureGeoDataLoaded();

    // Helper to calculate km in each province/ccaa
    const getSpatialKm = (pathPoints) => {
        const stats = new Map(); // "CCAA|Province" -> km
        if (!pathPoints || pathPoints.length < 2) return stats;

        for (let i = 0; i < pathPoints.length - 1; i++) {
            const p1 = pathPoints[i];
            const p2 = pathPoints[i+1];
            const dist = turf.distance(turf.point(p1), turf.point(p2));
            const mid = [ (p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2 ];
            const pointMid = turf.point(mid);

            let province = 'Unknown';
            let ccaa = 'Unknown';

            if (provincesGeoJSON) {
                for (const f of provincesGeoJSON.features) {
                    if (turf.booleanPointInPolygon(pointMid, f.geometry)) {
                        province = f.properties.name;
                        break;
                    }
                }
            }
            if (ccaaGeoJSON) {
                for (const f of ccaaGeoJSON.features) {
                    if (turf.booleanPointInPolygon(pointMid, f.geometry)) {
                        ccaa = f.properties.name;
                        break;
                    }
                }
            }
            const key = `${ccaa}|${province}`;
            stats.set(key, (stats.get(key) || 0) + dist);
        }
        return stats;
    };

    const lineStats = [];
    // latestDate is already defined above in the file collection loop

    // We need line names for discovered lines that don't have LTVs
    const lineNumToName = new Map();
    for (const fullName in enriched) {
        const m = fullName.match(/LÍNEA\s+(\d{3})/);
        if (m) lineNumToName.set(m[1], fullName);
    }

    for (const lineNum of allAdifLines) {
        // Ensure the line tramos are downloaded and enriched
        const tramos = await fetchLineTramos(lineNum);
        if (tramos.length === 0) continue;

        // Use a standardized name: "LÍNEA XXX"
        const lineName = `LÍNEA ${lineNum}`;
        const lineData = enriched[lineName] || [];
        // Support matching descriptive names from enriched too
        const enrichedMatchingNames = Object.keys(enriched).filter(k => k.startsWith(`LÍNEA ${lineNum}`));
        const allRelevantEnrichedData = [];
        enrichedMatchingNames.forEach(k => allRelevantEnrichedData.push(...enriched[k]));

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

        const lineId = lineNum;
        const lineGeoStats = new Map(); // "CCAA|Province" -> { totalKm: 0, ltvKm: 0, rawIntervals: [] }
        const allTramos = tramos;
        
        // 2. Spatial analysis of the full line geometry (per province/ccaa)
        for (const t of allTramos) {
            if (t.geography) {
                for (const geo of t.geography) {
                    const key = `${geo.ccaa}|${geo.province}`;
                    if (!lineGeoStats.has(key)) lineGeoStats.set(key, { totalKm: 0, ltvKm: 0, rawIntervals: [] });
                    lineGeoStats.get(key).rawIntervals.push([Math.min(geo.startPk, geo.endPk), Math.max(geo.startPk, geo.endPk)]);
                }
            }
        }

        // Merge Raw Intervals to avoid overcounting overlapping segments (e.g. double track)
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
                // Also need to avoid overcounting LTV on overlapping tramos!
                // For each CCAA/Province, we find the overlap of current LTV with merged track intervals
                for (const [key, stats] of lineGeoStats) {
                    // This is slightly complex: we should merge LTV intersections across all raw segments to avoid overcounting
                    // But an easier way: find which parts of the merged intervals of THIS CCAA/province are covered by the current LTV
                    if (stats.rawIntervals.length > 0) {
                        // We use the already merged track intervals for THIS province
                        // Re-merge them once for efficiency (already done above)
                        stats.rawIntervals.sort((a, b) => a[0] - b[0]);
                        let mergedTrack = [];
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

        const isHighSpeed = suffix.includes('_dhltv');
        lineStats.push({
            line: lineName,
            network: isHighSpeed ? 'high_speed' : 'conventional',
            totalLengthKm: Math.round(lineTotalKm * 100) / 100,
            ltvTotalKm: Math.round(totalLtvKm * 100) / 100,
            ltvPercentage: lineTotalKm > 0 ? Math.round((totalLtvKm / lineTotalKm) * 10000) / 100 : 0,
            geography: formattedBreakdown
        });
    }

    return { enriched, lineStats };
}

/**
 * Merges overlapping [start, end] intervals and returns the total length covered.
 */
function calculateIntervalSum(intervals) {
    if (intervals.length === 0) return 0;
    const sorted = intervals
        .map(i => [Math.min(i[0], i[1]), Math.max(i[0], i[1])])
        .sort((a, b) => a[0] - b[0]);
    
    let total = 0;
    let currentStart = sorted[0][0];
    let currentEnd = sorted[0][1];
    
    for (let i = 1; i < sorted.length; i++) {
        const nextStart = sorted[i][0];
        const nextEnd = sorted[i][1];
        if (nextStart <= currentEnd) {
            currentEnd = Math.max(currentEnd, nextEnd);
        } else {
            total += (currentEnd - currentStart);
            currentStart = nextStart;
            currentEnd = nextEnd;
        }
    }
    total += (currentEnd - currentStart);
    return total;
}

async function reprocess() {
    console.log('Reprocessing all JSONs...');
    
    console.log('--- Processing DSLTV ---');
    const { ltv, lineStats: ltvStats } = await processFilesBySuffix('_dsltv.json', LTV_JSON);
    
    console.log('--- Processing DHLTV ---');
    const LTV_AV_JSON = path.join(OUTPUT_DIR, 'ltv_av.json');
    const { av, lineStats: avStats } = await processFilesBySuffix('_dhltv.json', LTV_AV_JSON);
    
    // Merge stats (in case a line appears in both, e.g. same line ID in DSL and DHL)
    const combinedStatsMap = new Map();
    [...ltvStats, ...avStats].forEach(s => {
        const lineNumMatch = s.line.match(/LÍNEA\s+(\d{3})/);
        const lineId = lineNumMatch ? lineNumMatch[1] : s.line;
        
        if (!combinedStatsMap.has(lineId)) {
            combinedStatsMap.set(lineId, { ...s });
            combinedStatsMap.get(lineId).networks = [s.network];
            delete combinedStatsMap.get(lineId).network;
        } else {
            const existing = combinedStatsMap.get(lineId);
            // Update ltv km
            existing.ltvTotalKm = Math.round((existing.ltvTotalKm + s.ltvTotalKm) * 100) / 100;
            existing.ltvPercentage = existing.totalLengthKm > 0 ? Math.round((existing.ltvTotalKm / existing.totalLengthKm) * 10000) / 100 : 0;
            
            if (!existing.networks.includes(s.network)) {
                existing.networks.push(s.network);
            }
            
            // Merge geography
            const existingGeoMap = new Map();
            existing.geography.forEach(g => existingGeoMap.set(g.name, g));
            
            s.geography.forEach(g => {
                if (!existingGeoMap.has(g.name)) {
                    existing.geography.push(g);
                } else {
                    const eg = existingGeoMap.get(g.name);
                    eg.totalKm = Math.max(eg.totalKm, g.totalKm); // Length should be same, take max
                    eg.ltvKm = Math.round((eg.ltvKm + g.ltvKm) * 100) / 100;
                    eg.ltvPercentage = eg.totalKm > 0 ? Math.round((eg.ltvKm / eg.totalKm) * 10000) / 100 : 0;
                    
                    // Merge provinces
                    const provMap = new Map();
                    eg.provinces.forEach(p => provMap.set(p.name, p));
                    g.provinces.forEach(p => {
                        if (!provMap.has(p.name)) {
                            eg.provinces.push(p);
                        } else {
                            const ep = provMap.get(p.name);
                            ep.totalKm = Math.max(ep.totalKm, p.totalKm);
                            ep.ltvKm = Math.round((ep.ltvKm + p.ltvKm) * 100) / 100;
                            ep.ltvPercentage = ep.totalKm > 0 ? Math.round((ep.ltvKm / ep.totalKm) * 10000) / 100 : 0;
                        }
                    });
                }
            });
        }
    });

    // Final Network Cleanup: A line is high_speed ONLY if it's in the 01x-09x range AND (has LTVs in DHL or 0 LTVs in total)
    // Actually, following the user's rule and Line 100 case:
    const highSpeedRange = ['010', '012', '014', '016', '020', '022', '024', '030', '032', '034', '036', '040', '042', '044', '046', '050', '052', '054', '056', '060', '062', '064', '066', '068', '070', '072', '074', '076', '080', '082', '084', '982'];
    
    combinedStatsMap.forEach((s, lineId) => {
        const hasHistoryInAV = avStats.some(av => av.line.includes(lineId) && av.ltvTotalKm > 0);
        const isInHighSpeedRange = highSpeedRange.includes(lineId);
        
        if (hasHistoryInAV || (isInHighSpeedRange && !ltvStats.some(conv => conv.line.includes(lineId) && conv.ltvTotalKm > 0))) {
            s.networks = ['high_speed'];
        } else {
            s.networks = ['conventional'];
        }
    });
    const LINES_JSON = path.join(OUTPUT_DIR, 'lines.json');
    fs.writeFileSync(LINES_JSON, JSON.stringify(Array.from(combinedStatsMap.values()).sort((a,b) => a.line.localeCompare(b.line)), null, 2));
    console.log('Lines statistics generated in lines.json');

    return { ltv, av };
}

module.exports = { convertToJson, parseSinglePdf, reprocess, extractVigorDate, PDF_DIR, isWeekly };
