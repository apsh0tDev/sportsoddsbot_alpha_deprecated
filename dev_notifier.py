from discord_webhook import DiscordWebhook

webhook = DiscordWebhook(url="https://discord.com/api/webhooks/1252092770223657000/iuXCjYiNPq2UluyV_JlNwQuoheASgEs2c2X0Xwoq7UOfkSLTkc0-NIqBP6ZtU-5FAQ-D")

def notification(message):
    webhook.content = message
    webhook.execute()