def parse_resp(data):
    """
    PARSER RESP: Mengubah data mentah menjadi daftar (perintah, panjang_byte).
    Contoh output: [ (["SET", "a", "b"], 31), (["PING"], 14) ]
    """
    if not data: return []
    results = []
    cursor = 0
    
    while cursor < len(data):
        start = cursor
        # Redis RESP Array dimulai dengan '*'
        if data[cursor:cursor+1] == b'*':
            end_line = data.find(b"\r\n", cursor)
            if end_line == -1: break
            try:
                num_elements = int(data[cursor+1:end_line])
                cursor = end_line + 2
                cmd = []
                for _ in range(num_elements):
                    if data[cursor:cursor+1] != b'$': break
                    end_line = data.find(b"\r\n", cursor)
                    if end_line == -1: break
                    bulk_len = int(data[cursor+1:end_line])
                    cursor = end_line + 2
                    val = data[cursor:cursor+bulk_len].decode()
                    cmd.append(val)
                    cursor += bulk_len + 2
                results.append((cmd, cursor - start))
            except (ValueError, IndexError):
                break
        else:
            # Menangani Simple String atau perintah non-array lainnya
            end_line = data.find(b"\r\n", cursor)
            if end_line == -1: break
            results.append(([data[cursor:end_line].decode()], end_line - cursor + 2))
            cursor = end_line + 2
            
    return results

def format_xread_data(data):
    """
    Mengubah hasil temuan data Stream kita menjadi format Array RESP yang kompleks.
    Digunakan khusus untuk membalas perintah XREAD.
    """
    if not data: return "*-1\r\n"
    o = f"*{len(data)}\r\n"
    for k, ents in data:
        # Format: [ [key, [ [id, [fields]] ] ] ]
        o += f"*2\r\n${len(k)}\r\n{k}\r\n*{len(ents)}\r\n"
        for eid, flds in ents:
            o += f"*2\r\n${len(eid)}\r\n{eid}\r\n*{len(flds)*2}\r\n"
            for fk, fv in flds.items(): o += f"${len(fk)}\r\n{fk}\r\n${len(fv)}\r\n{fv}\r\n"
    return o
