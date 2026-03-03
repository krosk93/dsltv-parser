const fs = require('fs');
const path = require('path');
const { PDFParse } = require('pdf-parse');
const turf = require('@turf/turf');
const axios = require('axios');

const PDF_DIR = path.join(__dirname, 'pdfs');
const OUTPUT_DIR = path.join(__dirname, 'output');
const STATIONS_FILE = path.join(__dirname, 'stations.json');
const LTV_JSON = path.join(OUTPUT_DIR, 'ltv.json');

const WFS_BASE = 'https://ideadif.adif.es/gservices/Tramificacion/wfs';
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
                if (tableResult.pages && tableResult.pages[0] && tableResult.pages[0].tables) {
                    tableRows = tableResult.pages[0].tables[0];
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
                        code: codeMatch[1],
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
 * Interpolates coordinates between two PK points based on a target PK value.
 * If the target falls between pkA and pkB, linearly interpolates lat/lon.
 */
function interpolateCoords(pkA, pkB, targetPk) {
    if (pkA.pk === pkB.pk) return { lat: pkA.lat, lon: pkA.lon };
    const t = (targetPk - pkA.pk) / (pkB.pk - pkA.pk);
    return {
        lat: pkA.lat + t * (pkB.lat - pkA.lat),
        lon: pkA.lon + t * (pkB.lon - pkA.lon)
    };
}

/**
 * Finds the best coordinate for a given PK value from a sorted array of PK points.
 * Interpolates between the two nearest PKs if the target falls between them,
 * or returns the nearest PK if outside range.
 */
function findCoordsForPk(sortedPks, targetPk) {
    if (sortedPks.length === 0) return null;
    if (sortedPks.length === 1) return { lat: sortedPks[0].lat, lon: sortedPks[0].lon };

    // Find the two surrounding PKs for interpolation
    for (let i = 0; i < sortedPks.length - 1; i++) {
        if (targetPk >= sortedPks[i].pk && targetPk <= sortedPks[i + 1].pk) {
            return interpolateCoords(sortedPks[i], sortedPks[i + 1], targetPk);
        }
    }

    // Target PK is outside the range: use nearest PK
    const first = sortedPks[0];
    const last = sortedPks[sortedPks.length - 1];
    if (Math.abs(targetPk - first.pk) <= Math.abs(targetPk - last.pk)) {
        return { lat: first.lat, lon: first.lon };
    }
    return { lat: last.lat, lon: last.lon };
}

/**
 * Fetches codtramo prefixes for a given line number from the WFS TramosServicio layer.
 * The codtramo in ADIF WFS does not directly correspond to the LÍNEA number —
 * it uses an internal coding. This function resolves the mapping.
 */
async function fetchCodtramoPrefixes(lineNum) {
    const cacheFile = path.join(WFS_CACHE_DIR, `prefixes_${lineNum}.json`);

    // Check cache
    if (fs.existsSync(cacheFile)) {
        return JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
    }

    try {
        const url = `${WFS_BASE}?service=WFS&version=2.0.0&request=GetFeature` +
            `&typeName=Tramificacion:TramosServicio&outputFormat=application/json` +
            `&propertyName=codtramo,cod_linea` +
            `&CQL_FILTER=cod_linea LIKE '${lineNum} %25'`;
        const response = await axios.get(url, { timeout: 15000 });
        const data = response.data;
        if (!data.features || data.features.length === 0) return [];

        // Extract the unique 5-char codtramo prefixes (e.g. "01100" from "011000010")
        const prefixes = new Set();
        for (const f of data.features) {
            if (f.properties.codtramo) {
                prefixes.add(f.properties.codtramo.substring(0, 5));
            }
        }

        const result = Array.from(prefixes);

        // Save to cache
        fs.writeFileSync(cacheFile, JSON.stringify(result, null, 2));

        return result;
    } catch (err) {
        console.warn(`  WFS TramosServicio query failed for line ${lineNum}: ${err.message}`);
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
 * Main geocoding function. Uses a two-phase approach:
 * 1. WFS-based geocoding using ADIF's PKTeoricos for precise railway coordinates
 * 2. Station-based fallback for entries that WFS couldn't resolve
 */
async function geocode(ltvData) {
    console.log('Geocoding entries...');

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

        // For each line, resolve codtramo prefixes and fetch PK data
        const lineTramosCache = new Map(); // lineNum → Map(codtramo → sorted array of { pk, lat, lon })
        for (const lineNum of lineNumbers) {
            const prefixes = await fetchCodtramoPrefixes(lineNum);
            if (prefixes.length === 0) continue;

            const tramosMap = new Map();
            for (const prefix of prefixes) {
                const allPksForPrefix = await fetchPKTeoricos(prefix);
                // Group by full 9-digit codtramo to handle mileage resets
                for (const p of allPksForPrefix) {
                    if (!tramosMap.has(p.codtramo)) tramosMap.set(p.codtramo, []);
                    tramosMap.get(p.codtramo).push(p);
                }
            }

            // Sort individual tramos and store
            for (const [ct, pks] of tramosMap.entries()) {
                pks.sort((a, b) => a.pk - b.pk);
            }
            if (tramosMap.size > 0) {
                lineTramosCache.set(lineNum, tramosMap);
                console.log(`  Line ${lineNum}: loaded ${tramosMap.size} segments (tramos)`);
            }
        }

        // Apply WFS coordinates to each record
        for (const lineName in ltvData) {
            const match = lineName.match(/LÍNEA\s+(\d{3})/);
            if (!match) continue;
            const lineNum = match[1];
            const lineTramos = lineTramosCache.get(lineNum);
            if (!lineTramos) continue;

            for (const record of ltvData[lineName]) {
                const startKm = parseFloat(record.startKm);
                const endKm = parseFloat(record.endKm);
                if (isNaN(startKm) && isNaN(endKm)) continue;

                const targetPk = isNaN(startKm) ? endKm : startKm;

                // Identify candidates tramos that contain the target PK
                const candidates = [];
                for (const [ct, pks] of lineTramos.entries()) {
                    if (pks.length === 0) continue;
                    if (targetPk >= pks[0].pk && targetPk <= pks[pks.length - 1].pk) {
                        candidates.push({ codtramo: ct, pks: pks });
                    }
                }

                if (candidates.length === 0) {
                    wfsFailed++;
                    continue;
                }

                let bestTramo = candidates[0];

                // If we have mileage resets (multiple candidates), disambiguate using station proximity
                if (candidates.length > 1) {
                    const recordStations = record.stations.split(/[-\s]+/).filter(s => s.length > 2);
                    let refCoords = null;
                    for (const st of recordStations) {
                        const normSt = normalize(st);
                        if (stationMap.has(normSt)) {
                            refCoords = stationMap.get(normSt);
                            break;
                        }
                        // Try partial match if exact match fails
                        const partialMatch = stationList.find(s => s.norm.includes(normSt) || normSt.includes(s.norm));
                        if (partialMatch) {
                            refCoords = partialMatch.coords;
                            break;
                        }
                    }

                    if (refCoords) {
                        let minDistance = Infinity;
                        for (const cand of candidates) {
                            const midIdx = Math.floor(cand.pks.length / 2);
                            const sample = cand.pks[midIdx];
                            const dist = Math.sqrt(Math.pow(sample.lat - refCoords.lat, 2) + Math.pow(sample.lon - refCoords.lon, 2));
                            if (dist < minDistance) {
                                minDistance = dist;
                                bestTramo = cand;
                            }
                        }
                    }
                }

                const startCoords = !isNaN(startKm) ? findCoordsForPk(bestTramo.pks, startKm) : null;
                const endCoords = !isNaN(endKm) ? findCoordsForPk(bestTramo.pks, endKm) : null;

                if (startCoords && endCoords) {
                    record.latitude = (startCoords.lat + endCoords.lat) / 2;
                    record.longitude = (startCoords.lon + endCoords.lon) / 2;
                    record.geocodingMethod = 'wfs';
                    wfsResolved++;
                } else if (startCoords || endCoords) {
                    const c = startCoords || endCoords;
                    record.latitude = c.lat;
                    record.longitude = c.lon;
                    record.geocodingMethod = 'wfs';
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

async function reprocess() {
    console.log('Reprocessing all JSONs...');
    const files = fs.readdirSync(PDF_DIR)
        .filter(f => f.toLowerCase().endsWith('_dsltv.json'))
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

    let enriched = await geocode(sortedGrouped);
    enriched = await reverseGeocode(enriched);

    fs.writeFileSync(LTV_JSON, JSON.stringify(enriched, null, 2));
    console.log('Regeneration complete.');
    return enriched;
}

module.exports = { convertToJson, parseSinglePdf, reprocess, extractVigorDate, PDF_DIR, isWeekly };
