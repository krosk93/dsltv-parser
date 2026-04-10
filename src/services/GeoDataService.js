const fs = require('fs');
const turf = require('@turf/turf');
const GeometryUtils = require('../utils/GeometryUtils');
const { PROVINCES_FILE, COMMUNITIES_FILE } = require('../core/Config');

class GeoDataService {
    constructor() {
        this.provincesGeoJSON = null;
        this.ccaaGeoJSON = null;
        this.isLoaded = false;
    }

    load() {
        if (this.isLoaded) return;
        this.isLoaded = true; // Mark as loaded immediately to avoid redundant checks
        try {
            if (fs.existsSync(PROVINCES_FILE)) {
                this.provincesGeoJSON = JSON.parse(fs.readFileSync(PROVINCES_FILE, 'utf8'));
            }
            if (fs.existsSync(COMMUNITIES_FILE)) {
                this.ccaaGeoJSON = JSON.parse(fs.readFileSync(COMMUNITIES_FILE, 'utf8'));
            }
        } catch (e) {
            console.warn('GeoJSON data not found or invalid');
        }
        this.isLoaded = true;
    }

    getGeoInfoForPoint(point) {
        this.load();
        let province = 'Unknown';
        let ccaa = 'Unknown';

        const turfPoint = Array.isArray(point) ? turf.point(point) : point;

        if (this.provincesGeoJSON) {
            for (const f of this.provincesGeoJSON.features) {
                if (turf.booleanPointInPolygon(turfPoint, f.geometry)) {
                    province = f.properties.name;
                    break;
                }
            }
        }

        if (this.ccaaGeoJSON) {
            for (const f of this.ccaaGeoJSON.features) {
                if (turf.booleanPointInPolygon(turfPoint, f.geometry)) {
                    ccaa = f.properties.name;
                    break;
                }
            }
        }

        return { province, ccaa };
    }

    enrichTramoWithGeo(tramo) {
        this.load();
        if (!this.provincesGeoJSON || !this.ccaaGeoJSON) return;

        const line = GeometryUtils.extractLineString(tramo.geometry);
        if (!line) return;

        const coords = line.coordinates;
        const geoIntervals = [];
        if (coords.length < 2) return;

        const pStartKm = tramo.pki;
        const pEndKm = tramo.pkd;

        let lastKey = null;
        let segmentStartKm = pStartKm;

        for (let i = 0; i < coords.length - 1; i++) {
            const p1 = coords[i];
            const p2 = coords[i+1];
            const mid = [ (p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2 ];
            const pointMid = turf.point(mid);
            const ratio = i / (coords.length - 1);
            const currentPk = pStartKm + (pEndKm - pStartKm) * ratio;

            const { province, ccaa } = this.getGeoInfoForPoint(pointMid);
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
}

module.exports = new GeoDataService();
