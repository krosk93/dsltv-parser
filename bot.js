const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const { reprocess, extractVigorDate, PDF_DIR } = require('./processor');

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
        bot.sendMessage(chatId, 'Por favor, envía un archivo PDF.');
        return;
    }

    bot.sendMessage(chatId, 'Procesando tu PDF, por favor espera...');

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

        // Extract date
        const vigorDate = await extractVigorDate(tempPath);
        if (!vigorDate) {
            bot.sendMessage(chatId, 'Vaya, el PDF no parece ser un DSLTV válido. ¿Podrías verificarlo?');
            fs.unlinkSync(tempPath);
            return;
        }

        const newFileName = `${vigorDate}_DSLTV.pdf`;
        const finalPath = path.join(PDF_DIR, newFileName);

        if (fs.existsSync(finalPath)) {
            bot.sendMessage(chatId, `¡Gracias por tu aportación!`);
            return;
        }

        fs.renameSync(tempPath, finalPath);
        bot.sendMessage(chatId, `Procesando...`);

        await reprocess();
        bot.sendMessage(chatId, `¡Gracias por tu aportación!`);

    } catch (error) {
        console.error('Error processing bot request:', error);
        bot.sendMessage(chatId, 'Ha ocurrido un error procesando el archivo. ¿Podrías intentarlo de nuevo?');
    }
});

bot.onText(/\/start/, (msg) => {
    bot.sendMessage(msg.chat.id, '¡Bienvenido! Puedes enviarme los PDFs de DSLTV y los procesaré automáticamente.');
});
