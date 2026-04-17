def parse_resp(data):
    """
    RESP Parser: Converts raw binary data into a list of (command_parts, byte_length).
    Example output: [ (["SET", "key", "value"], 31), (["PING"], 14) ]
    """
    if not data: return []
    results = []
    cursor = 0
    
    while cursor < len(data):
        start = cursor
        # Redis RESP Array starts with '*'
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
            # Handle Simple Strings or other non-array commands
            end_line = data.find(b"\r\n", cursor)
            if end_line == -1: break
            results.append(([data[cursor:end_line].decode()], end_line - cursor + 2))
            cursor = end_line + 2
            
    return results

def format_xread_data(data):
    """
    Formats internal stream data into a complex nested RESP Array.
    Used specifically for responding to XREAD commands.
    """
    if not data: return "*-1\r\n"
    res = f"*{len(data)}\r\n"
    for stream_name, entries in data:
        # Structure: [ [stream_name, [ [entry_id, [field_data]] ] ] ]
        res += f"*2\r\n${len(stream_name)}\r\n{stream_name}\r\n*{len(entries)}\r\n"
        for entry_id, fields in entries:
            res += f"*2\r\n${len(entry_id)}\r\n{entry_id}\r\n*{len(fields)*2}\r\n"
            for field_name, field_value in fields.items():
                res += f"${len(field_name)}\r\n{field_name}\r\n${len(field_value)}\r\n{field_value}\r\n"
    return res
