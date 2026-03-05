// minimal-bot.js
import { Client, GatewayIntentBits, Events } from 'discord.js';
import dotenv from 'dotenv';
dotenv.config();

const TOKEN = process.env.DISCORD_TOKEN;
if (!TOKEN) throw new Error('DISCORD_TOKEN is required');
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("Web server running on port", PORT);
});
const client = new Client({ intents: [GatewayIntentBits.Guilds] });

client.once(Events.ClientReady, () => {
    console.log(`READY! Logged in as ${client.user.tag}`);
});

client.on(Events.Debug, console.log);
client.on(Events.Warn, console.log);
client.on(Events.Error, console.error);

console.log("LOGIN START");
client.login(TOKEN);
