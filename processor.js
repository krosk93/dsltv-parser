const fs = require('fs');
const path = require('path');
const { PDFParse } = require('pdf-parse');

const PDF_DIR = path.join(__dirname, 'pdfs');
const OUTPUT_DIR = path.join(__dirname, 'output');
const STATIONS_FILE = path.join(__dirname, 'stations.json');
const LTV_JSON = path.join(OUTPUT_DIR, 'ltv.json');

if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR);

function normalize(text) {
    if (!text) return '';
    return text.toString()
        .toLowerCase()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/'/g, '')
        .replace(/\b(madrid|barcelona|valencia|sevilla|donostia|bilbao|barna|irun)-\b/g, '')
        .replace(/\b(apd|cgd|estacion|mercan-|mercan|p\.k\.|pk|bif\.)\b/g, '')
        .replace(/[^a-z0-9]/g, ' ')
        .replace(/\s+/g, ' ')
        .replace('glories', 'glorias')
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
        const match = result.text.match(/(\d{2})\/(\d{2})\/(\d{4}).*Fecha Vigor/i);
        if (match) {
            return `${match[3]}${match[2]}${match[1]}`; // YYYYMMDD
        }
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

async function geocode(ltvData) {
    console.log('Geocoding entries...');
    if (!fs.existsSync(STATIONS_FILE)) {
        console.warn('stations.json not found, skipping geocoding');
        return ltvData;
    }
    const stationsData = JSON.parse(fs.readFileSync(STATIONS_FILE, 'utf8'));
    const stationMap = new Map();
    for (const s of stationsData) {
        if (s.DESCRIPCION && s.LATITUD && s.LONGITUD) {
            const norm = normalize(s.DESCRIPCION);
            if (norm.length > 0 && !stationMap.has(norm)) {
                stationMap.set(norm, { lat: s.LATITUD, lon: s.LONGITUD });
            }
        }
    }
    const stationList = Array.from(stationMap.entries()).map(([norm, coords]) => ({ norm, coords })).sort((a, b) => a.norm.length - b.norm.length);

    for (const lineName in ltvData) {
        for (const record of ltvData[lineName]) {
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
            }
        }
    }
    return ltvData;
}

async function reprocess() {
    console.log('Reprocessing all PDFs...');
    const files = fs.readdirSync(PDF_DIR)
        .filter(f => f.toLowerCase().endsWith('.pdf'))
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
        const records = await parseSinglePdf(file.path);
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

    const enriched = await geocode(sortedGrouped);
    fs.writeFileSync(LTV_JSON, JSON.stringify(enriched, null, 2));
    console.log('Regeneration complete.');
    return enriched;
}

module.exports = { reprocess, extractVigorDate, PDF_DIR };
