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
        return JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
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

    try {
        console.log('  Phase 1: Querying ADIF WFS for precise railway coordinates...');

        // Collect unique line numbers
        const lineNumbers = new Set();
        for (const lineName in ltvData) {
            const match = lineName.match(/LÍNEA\s+(\d{3})/);
            if (match) lineNumbers.add(match[1]);
        }
        console.log(`  Found ${lineNumbers.size} unique line numbers`);

        // For each line, resolve tramos with geometry
        const lineTramosCache = new Map(); // lineNum → Array of Tramos
        for (const lineNum of lineNumbers) {
            const tramos = await fetchLineTramos(lineNum);
            if (tramos.length > 0) {
                lineTramosCache.set(lineNum, tramos);
                console.log(`  Line ${lineNum}: loaded ${tramos.length} segments with geometry`);
            }
        }

        // Apply WFS coordinates to each record
        for (const lineName in ltvData) {
            const match = lineName.match(/LÍNEA\s+(\d{3})/);
            if (!match) continue;
            const lineNum = match[1];
            const allTramos = lineTramosCache.get(lineNum);
            if (!allTramos) continue;

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
    return ltvData;
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
    for (const file of files) {
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

    let enriched = await geocode(sortedGrouped, suffix.includes('_dhltv'));
    enriched = await reverseGeocode(enriched);

    fs.writeFileSync(outputPath, JSON.stringify(enriched, null, 2));
    console.log(`Regeneration complete for ${suffix}.`);
    return enriched;
}

async function reprocess() {
    console.log('Reprocessing all JSONs...');
    
    console.log('--- Processing DSLTV ---');
    const ltv = await processFilesBySuffix('_dsltv.json', LTV_JSON);
    
    console.log('--- Processing DHLTV ---');
    const LTV_AV_JSON = path.join(OUTPUT_DIR, 'ltv_av.json');
    const av = await processFilesBySuffix('_dhltv.json', LTV_AV_JSON);
    
    return { ltv, av };
}

module.exports = { convertToJson, parseSinglePdf, reprocess, extractVigorDate, PDF_DIR, isWeekly };
