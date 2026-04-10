const turf = require('@turf/turf');

class GeometryUtils {
    /**
     * Extracts a single LineString from a geometry (LineString or MultiLineString).
     */
    static extractLineString(geometry) {
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
    static findCoordsOnTramo(tramo, targetPk) {
        const line = this.extractLineString(tramo.geometry);
        if (!line) return null;

        const range = tramo.pkd - tramo.pki;
        if (Math.abs(range) < 0.0001) {
            return { lon: line.coordinates[0][0], lat: line.coordinates[0][1] };
        }

        const length = turf.length(line);
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
    static sliceTramoByPk(tramo, startPk, endPk) {
        const line = this.extractLineString(tramo.geometry);
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
     * Merges overlapping [start, end] intervals and returns the total length covered.
     */
    static calculateIntervalSum(intervals) {
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

    /**
     * Calculates the extra time (delay) in seconds for a speed limitation,
     * including deceleration and acceleration phases.
     */
    static calculateEnhancedDelay(ltvSpeedKmh, designSpeedKmh, distanceKm, isAv = false) {
        if (ltvSpeedKmh >= designSpeedKmh || ltvSpeedKmh <= 0) return 0;

        const Vd = designSpeedKmh / 3.6; // m/s
        const Vl = ltvSpeedKmh / 3.6;   // m/s
        const distanceM = distanceKm * 1000;

        // 1. Constant speed phase delay
        const delayConstant = distanceM * (1 / Vl - 1 / Vd);

        // 2. Deceleration phase delay
        let aDec = 1.0;
        if (isAv) {
            aDec = Math.pow(320 / 3.6, 2) / 16000;
        }
        const delayDec = Math.pow(Vd - Vl, 2) / (2 * aDec * Vd);

        // 3. Acceleration phase delay
        let delayAcc = 0;
        if (isAv) {
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

            const getAccForV = (vKmh) => {
                for (let i = 0; i < accTable.length - 1; i++) {
                    if (vKmh >= accTable[i].v && vKmh <= accTable[i + 1].v) {
                        const t = (vKmh - accTable[i].v) / (accTable[i + 1].v - accTable[i].v);
                        return accTable[i].a + t * (accTable[i + 1].a - accTable[i].a);
                    }
                }
                return accTable[accTable.length - 1].a;
            };

            const step = 0.5; // km/h
            for (let v = ltvSpeedKmh; v < designSpeedKmh; v += step) {
                const vMs = v / 3.6;
                const a = getAccForV(v);
                delayAcc += ((step / 3.6) / a) * (1 - vMs / Vd);
            }
        }

        return Math.round((delayConstant + delayDec + delayAcc) * 10) / 10;
    }
}

module.exports = GeometryUtils;
