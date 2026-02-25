const { reprocess } = require('./processor');

reprocess()
    .then(() => console.log('Successfully reprocessed all data.'))
    .catch(err => console.error('Processing failed:', err));
