def parse_resp(data):
    """Menerjemahkan data mentah RESP menjadi daftar perintah Python"""
    if not data: return []
    try:
        lines = data.decode().split("\r\n")
    except UnicodeDecodeError: return []
    
    commands = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line:
            i += 1
            continue
        if line.startswith('*'):
            try:
                num_args = int(line[1:])
                cmd_parts = []
                i += 1
                for _ in range(num_args):
                    if i + 1 < len(lines):
                        cmd_parts.append(lines[i+1])
                        i += 2
                if len(cmd_parts) == num_args:
                    commands.append(cmd_parts)
            except (ValueError, IndexError):
                i += 1
        else: i += 1
    return commands

def format_xread_data(data):
    """Memformat data Stream ke Array RESP untuk XREAD"""
    if not data: return "*-1\r\n"
    o = f"*{len(data)}\r\n"
    for k, ents in data:
        o += f"*2\r\n${len(k)}\r\n{k}\r\n*{len(ents)}\r\n"
        for eid, flds in ents:
            o += f"*2\r\n${len(eid)}\r\n{eid}\r\n*{len(flds)*2}\r\n"
            for fk, fv in flds.items(): o += f"${len(fk)}\r\n{fk}\r\n${len(fv)}\r\n{fv}\r\n"
    return o
