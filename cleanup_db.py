import pathlib

path = pathlib.Path(r'c:\Users\user\Desktop\Telegram bot\bot.py')
text = path.read_text(encoding='utf-8').splitlines()
start = None
for i,l in enumerate(text):
    if l.strip().startswith('class Database:'):
        start=i
        break
end = None
for j in range(start+1, len(text)):
    if text[j].startswith('# ------------------- API RATE LIMITING'):
        end = j
        break
if start is not None and end is not None:
    fetchrow_end=None
    for k in range(start, end):
        if 'return await conn.fetchrow' in text[k]:
            fetchrow_end = k
            break
    if fetchrow_end is None:
        print('could not find fetchrow_end')
    else:
        new_lines = text[:fetchrow_end+1] + text[end:]
        path.write_text("\n".join(new_lines), encoding='utf-8')
        print(f'Removed duplicates from lines {fetchrow_end+1} to {end-1}')
else:
    print('start or end not found')
