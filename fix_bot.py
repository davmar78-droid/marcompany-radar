import re

with open('bot.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Arreglar comillas tipograficas de Mac
replacements = {
    '\u2018': "'", '\u2019': "'",
    '\u201c': '"', '\u201d': '"',
    '\u2013': '-', '\u2014': '-',
}
for bad, good in replacements.items():
    content = content.replace(bad, good)

# Poner el numero y apikey
content = re.sub(r'"whatsapp_phone":\s*"[^"]*"', '"whatsapp_phone":   "+34669713968"', content)
content = re.sub(r'"callmebot_apikey":\s*"[^"]*"', '"callmebot_apikey": "TU_API_KEY"', content)

with open('bot.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("bot.py arreglado correctamente")
