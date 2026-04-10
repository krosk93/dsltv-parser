const path = require('path');

const ROOT_DIR = path.resolve(__dirname, '../../');
const PDF_DIR = path.join(ROOT_DIR, 'pdfs');
const OUTPUT_DIR = path.join(ROOT_DIR, 'output');
const WFS_CACHE_DIR = path.join(ROOT_DIR, 'wfs');
const STATIONS_FILE = path.join(ROOT_DIR, 'stations.json');
const COMMUNITIES_FILE = path.join(ROOT_DIR, 'communities.geojson');
const PROVINCES_FILE = path.join(ROOT_DIR, 'provinces.geojson');

module.exports = {
    ROOT_DIR,
    PDF_DIR,
    OUTPUT_DIR,
    WFS_CACHE_DIR,
    STATIONS_FILE,
    COMMUNITIES_FILE,
    PROVINCES_FILE,
    LTV_JSON: path.join(OUTPUT_DIR, 'ltv.json'),
    LTV_AV_JSON: path.join(OUTPUT_DIR, 'ltv_av.json'),
    LINES_JSON: path.join(OUTPUT_DIR, 'lines.json'),
    WFS_BASE: 'https://ideadif.adif.es/gservices/Tramificacion/wfs',
    DESIGN_SPEED_WFS: 'https://ideadif.adif.es/services/wfs',
    HIGH_SPEED_RANGE: [
        '010', '012', '014', '016', '020', '022', '024', '030', '032', '034', '036', 
        '040', '042', '044', '046', '050', '052', '054', '056', '060', '062', '064', 
        '066', '068', '070', '072', '074', '076', '080', '082', '084', '982'
    ]
};
