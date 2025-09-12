class CommandParser:
    
    def _parse_bulk_string(self, buffer, s_len):
        if len(buffer) < s_len + 2:  # +2 for \r\n
            return None, buffer, 0  # Not enough data
        
        try:
            bulk_str = buffer[:s_len].decode('utf-8')
            remaining_buffer = buffer[s_len + 2:]  # +2 for \r\n
            bytes_processed = s_len + 2
            return bulk_str, remaining_buffer, bytes_processed
        except UnicodeDecodeError:
            # If we can't decode it, it might be binary data (RDB file)
            print("Skipping binary data (likely RDB)")
            remaining_buffer = buffer[s_len + 2:]
            return None, remaining_buffer, s_len + 2


    def _parse_array(self, buffer, num_args):
        elements = []
        current_buffer = buffer
        total_bytes = 0
        
        for _ in range(num_args):
            element, current_buffer, bytes_processed = self._parse_stream(current_buffer)
            if element is None and bytes_processed == 0:
                return None, buffer, 0  # Not enough data
            total_bytes += bytes_processed
            if element is not None:
                elements.append(element)

        if not elements:
            return None, current_buffer, total_bytes
            
        return elements, current_buffer, total_bytes


    def _parse_stream(self, buffer):
        if not buffer:
            return None, buffer, 0
            
        # Find the first \r\n
        crlf_pos = buffer.find(b'\r\n')
        if crlf_pos == -1:
            return None, buffer, 0  # Not enough data
            
        header = buffer[:crlf_pos].decode()
        remaining_buffer = buffer[crlf_pos + 2:]
        cmd_type = header[0]
        header_bytes = len(header) + 2
        
        if cmd_type == '*':
            num_args = int(header[1:])
            result, remaining_buffer, bytes_processed = self._parse_array(remaining_buffer, num_args)
            return result, remaining_buffer, header_bytes + bytes_processed
        elif cmd_type == '$':
            s_len = int(header[1:])
            if s_len >= 0:
                result, remaining_buffer, bytes_processed = self._parse_bulk_string(remaining_buffer, s_len)
                return result, remaining_buffer, header_bytes + bytes_processed
            else:
                return None, remaining_buffer, header_bytes  # Null bulk string
        else:
            return None, remaining_buffer, header_bytes


    def parse_commands(self, buffer):
        commands = []
        current_buffer = buffer
        
        while current_buffer:
            command, new_buffer, bytes_processed = self._parse_stream(current_buffer)

            if bytes_processed == 0:
                break # Not enough data to parse a full command
                
            if command is not None and isinstance(command, list) and len(command) > 0:
                commands.append((command, bytes_processed))

            current_buffer = new_buffer
            
        return commands, current_buffer