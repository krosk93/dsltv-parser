const fs = require('fs');
const Fuse = require('fuse.js');
const TextNormalizer = require('../utils/TextNormalizer');
const { STATIONS_FILE } = require('../core/Config');

class StationService {
    constructor() {
        this.stationMap = new Map();
        this.stationList = [];
        this.fuse = null;
        this.isLoaded = false;
    }

    load() {
        if (this.isLoaded) return;
        if (fs.existsSync(STATIONS_FILE)) {
            const stationsData = JSON.parse(fs.readFileSync(STATIONS_FILE, 'utf8'));
            for (const s of stationsData) {
                if (s.DESCRIPCION && s.LATITUD && s.LONGITUD) {
                    const norm = TextNormalizer.normalize(s.DESCRIPCION);
                    if (norm.length > 0 && !this.stationMap.has(norm)) {
                        this.stationMap.set(norm, { lat: s.LATITUD, lon: s.LONGITUD });
                    }
                }
            }
        }
        this.stationList = Array.from(this.stationMap.entries())
            .map(([norm, coords]) => ({ norm, coords }))
            .sort((a, b) => a.norm.length - b.norm.length);
        
        this.fuse = new Fuse(this.stationList, { 
            keys: ['norm'], 
            threshold: 0.3, 
            distance: 100 
        });
        
        this.isLoaded = true;
    }

    findStation(name) {
        this.load();
        const normName = TextNormalizer.normalize(name);
        if (normName.length === 0) return null;

        // Exact match
        if (this.stationMap.has(normName)) {
            return this.stationMap.get(normName);
        }

        // Common variations
        const alts = [
            normName.replace(/^v/, 'b'), 
            normName.replace(/^b/, 'v'), 
            normName.replace('errenteria', 'renteria')
        ];
        for (const alt of alts) {
            if (this.stationMap.has(alt)) {
                return this.stationMap.get(alt);
            }
        }

        // Fuzzy match
        const fuzzy = this.fuse.search(normName);
        if (fuzzy.length > 0) {
            return fuzzy[0].item.coords;
        }

        // Partial match fallback
        const partial = this.stationList.find(s => s.norm.includes(normName) || normName.includes(s.norm));
        if (partial) return partial.coords;

        return null;
    }

    getCoordinatesForStations(names) {
        this.load();
        const foundCoords = [];
        for (const name of names) {
            const coords = this.findStation(name);
            if (coords) foundCoords.push(coords);
        }
        return foundCoords;
    }
}

module.exports = new StationService();
