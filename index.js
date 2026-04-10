const LTVProcessor = require('./src/services/LTVProcessor');

const processor = new LTVProcessor();

processor.reprocess()
    .then(() => console.log('Successfully reprocessed all data using the new OO architecture.'))
    .catch(err => {
        console.error('Processing failed:', err);
        process.exit(1);
    });
