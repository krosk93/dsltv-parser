const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
const fs = require('fs-extra');
const path = require('path');
const { convertToJson, parseSinglePdf, reprocess, extractVigorDate, PDF_DIR } = require('./processor');

// Replace with your actual token or use process.env.TOKEN
const token = process.env.TELEGRAM_BOT_TOKEN;

if (!token) {
    console.error('Please set TELEGRAM_BOT_TOKEN environment variable');
    process.exit(1);
}

const bot = new TelegramBot(token, { polling: true });

console.log('Bot is running...');

bot.on('document', async (msg) => {
    const chatId = msg.chat.id;
    const fileId = msg.document.file_id;
    const fileName = msg.document.file_name;

    if (!fileName.toLowerCase().endsWith('.pdf')) {
        bot.sendMessage(chatId, 'Si us plau, envia un fitxer PDF.');
        bot.deleteMessage(chatId, msg.message_id);
        return;
    }

    bot.sendMessage(chatId, 'Processant el fitxer PDF, si us plau, espera...');

    try {
        const fileLink = await bot.getFileLink(fileId);
        const tempPath = path.join(__dirname, 'temp', fileName);

        // Download file
        const response = await axios({
            url: fileLink,
            method: 'GET',
            responseType: 'stream'
        });

        const writer = fs.createWriteStream(tempPath);
        response.data.pipe(writer);

        await new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
        });

        bot.deleteMessage(chatId, msg.message_id);

        // Extract date
        const vigorDate = await extractVigorDate(tempPath);
        if (!vigorDate) {
            bot.sendMessage(chatId, 'Vaja, el PDF no sembla ser un DSLTV vàlid. Si us plau, verifica-ho.');
            fs.unlinkSync(tempPath);
            return;
        }

        const newFileName = `${vigorDate}_DSLTV.json`;
        const finalPath = path.join(PDF_DIR, newFileName);

        if (fs.existsSync(finalPath)) {
            bot.sendMessage(chatId, `Gracies per a la teva contribució!`);
            return;
        }

        const parsedData = await parseSinglePdf(tempPath);

        if (parsedData.length === 0) {
            bot.sendMessage(chatId, 'Vaja, el PDF no sembla ser un DSLTV vàlid. Si us plau, verifica-ho.');
            fs.unlinkSync(tempPath);
            return;
        }

        fs.writeFileSync(finalPath, JSON.stringify(parsedData, null, 2));
        fs.unlinkSync(tempPath);

        await reprocess();
        bot.sendMessage(chatId, `Gracies per a la teva contribució!`);

    } catch (error) {
        console.error('Error processing bot request:', error);
        bot.sendMessage(chatId, 'Sembla que no hem pogut processar el fitxer. Pots intentar-ho de nou?');
    }
});

bot.onText(/\/start/, (msg) => {
    bot.sendMessage(msg.chat.id, 'Benvingut! Pots enviar-me PDFs de DSLTV i els processaré automàticament. Un cop processat, esborraré el PDF.');
});

convertToJson();
