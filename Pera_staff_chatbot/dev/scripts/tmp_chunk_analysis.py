import json

with open('assets/index/chunks.jsonl', 'r', encoding='utf-8') as f:
    rows = [json.loads(l) for l in f if l.strip()]

active = [r for r in rows if r.get('active', True)]
mp = [r for r in active if r.get('loc_start') != r.get('loc_end')]
rt = [r for r in active if '[Role:' in (r.get('text') or '')]
cwp = [r for r in active if 'compiled' in (r.get('doc_name') or '').lower()]

auth = {}
for r in active:
    a = r.get('doc_authority', 2)
    auth[a] = auth.get(a, 0) + 1

print('=== FINAL VERIFICATION METRICS ===')
print('Total chunks:', len(active))
print('Multi-page chunks:', len(mp), '(was: 0)')
print('Role-tagged chunks:', len(rt), '(was: 3)')
print('CWP chunks:', len(cwp), '/', len(active))
if cwp:
    print('CWP authority:', cwp[0].get('doc_authority'))
print('Authority dist:', auth)
print()

roles = set()
for r in rt:
    for l in r['text'].split('\n'):
        if l.startswith('[Role:'):
            roles.add(l.split(']')[0] + ']')
            break
print('Unique roles:', len(roles))
for role in sorted(roles)[:10]:
    print(' ', role)
