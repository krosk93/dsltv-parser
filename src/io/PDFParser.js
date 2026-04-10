const fs = require('fs');
const { PDFParse } = require('pdf-parse');
const TextNormalizer = require('../utils/TextNormalizer');

class PDFParser {
    async extractVigorDate(filePath) {
        const dataBuffer = fs.readFileSync(filePath);
        const parser = new PDFParse({ data: dataBuffer });
        try {
            const result = await parser.getText({ first: 2 });
            const match = result.text.match(/(\d{2})\/(\d{2})\/(\d{4})\s(\d{2})\:(\d{2}).*Fecha Vigor/i);
            if (match) {
                return `${match[3]}${match[2]}${match[1]}_${match[4]}${match[5]}`; // YYYYMMDD_HHMM
            }
        } catch (err) {
            console.error(`Error extracting date from ${filePath}:`, err);
        } finally {
            await parser.destroy();
        }
        return null;
    }

    async isWeekly(filePath) {
        const dataBuffer = fs.readFileSync(filePath);
        const parser = new PDFParse({ data: dataBuffer });
        try {
            const result = await parser.getText({ partial: [1] });
            return result.text.includes('SEMANAL');
        } catch (err) {
            console.error(`Error checking if ${filePath} is weekly:`, err);
        } finally {
            await parser.destroy();
        }
        return null;
    }

    async parse(filePath) {
        const dataBuffer = fs.readFileSync(filePath);
        const parser = new PDFParse({ data: dataBuffer });
        const records = [];
        let currentLine = 'Unknown Line';

        try {
            const info = await parser.getInfo();
            const totalPages = info.total;
            // The original code checks isWeekly again inside parseSinglePdf but doesn't store it in the record.
            // It's used to determine some logic in geocoding later (isAv).

            for (let i = 1; i <= totalPages; i++) {
                let tableRows = [];
                try {
                    const tableResult = await parser.getTable({ partial: [i] });
                    if (tableResult.pages && tableResult.pages[0] && tableResult.pages[0].tables && tableResult.pages[0].tables.length > 0) {
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
                            code: TextNormalizer.hashCode(codeMatch[1]),
                            stations: TextNormalizer.clean(row[1]),
                            track: TextNormalizer.clean(row[2]),
                            startKm: TextNormalizer.clean(row[3]),
                            endKm: TextNormalizer.clean(row[4]),
                            speed: TextNormalizer.clean(row[5]),
                            reason: TextNormalizer.clean(row[6]),
                            startDateTime: TextNormalizer.formatDateTime(row[7], row[8]),
                            endDateTime: TextNormalizer.formatDateTime(row[9], row[10]),
                            schedule: TextNormalizer.clean(row[11]),
                            csv: TextNormalizer.clean(row[14]) === 'X',
                            comment: TextNormalizer.clean(row[15])
                        });
                    }
                }
            }
        } finally {
            await parser.destroy();
        }
        return records;
    }
}

module.exports = new PDFParser();
