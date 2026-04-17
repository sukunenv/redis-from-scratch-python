import app.store as store
import hashlib

def handle_auth(cmd_name, cmd_parts, target, session):
    """
    Handles Authentication and ACL commands (ACL, AUTH, etc.)
    Manages user credentials and session authentication state.
    """
    # Initialize internal user store if not present
    if not hasattr(store, "USERS"):
        store.USERS = {
            "default": {
                "flags": ["nopass"],
                "passwords": []
            }
        }

    def arg(idx): return cmd_parts[idx].upper() if idx < len(cmd_parts) else None

    if cmd_name == "ACL":
        subcommand = arg(1)
        if subcommand == "WHOAMI":
            # Return the username of the current authenticated session
            current_user = session.get("authenticated_user", "default")
            target.sendall(f"${len(current_user)}\r\n{current_user}\r\n".encode())
            return True
            
        elif subcommand == "GETUSER":
            # Retrieve user details from the internal user store
            username = cmd_parts[2] if len(cmd_parts) > 2 else "default"
            user = store.USERS.get(username, {"flags": [], "passwords": []})
            
            flags = user["flags"]
            passwords = user["passwords"]
            
            # Construct a nested RESP Array response
            res = "*4\r\n$5\r\nflags\r\n"
            res += f"*{len(flags)}\r\n"
            for f in flags:
                res += f"${len(f)}\r\n{f}\r\n"
                
            res += "$9\r\npasswords\r\n"
            res += f"*{len(passwords)}\r\n"
            for p in passwords:
                res += f"${len(p)}\r\n{p}\r\n"
                
            target.sendall(res.encode())
            return True
            
        elif subcommand == "SETUSER":
            # Format: ACL SETUSER username [rules...]
            username = cmd_parts[2]
            if username not in store.USERS:
                store.USERS[username] = {"flags": [], "passwords": []}
                
            user = store.USERS[username]
            
            # Process rules sequentially
            for rule in cmd_parts[3:]:
                # '>' indicates a new password assignment
                if rule.startswith(">"):
                    password = rule[1:]
                    # Hash the password using SHA-256 for secure storage
                    hashed_pwd = hashlib.sha256(password.encode()).hexdigest()
                    if hashed_pwd not in user["passwords"]:
                        user["passwords"].append(hashed_pwd)
                        
                    # Remove 'nopass' flag once a password is set
                    if "nopass" in user["flags"]:
                        user["flags"].remove("nopass")
                        
            target.sendall(b"+OK\r\n")
            return True

    elif cmd_name == "AUTH":
        # Format: AUTH username password (or AUTH password for legacy clients)
        if len(cmd_parts) >= 3:
            username = cmd_parts[1]
            password = cmd_parts[2]
        else:
            username = "default"
            password = cmd_parts[1] if len(cmd_parts) > 1 else ""
            
        user = store.USERS.get(username, {"flags": [], "passwords": []})
        
        # Check for passwordless authentication eligibility
        if "nopass" in user["flags"]:
            session["authenticated_user"] = username
            target.sendall(b"+OK\r\n")
            return True
            
        # Hash input password to compare with the stored hashes
        hashed_input = hashlib.sha256(password.encode()).hexdigest()
        
        if hashed_input in user["passwords"]:
            session["authenticated_user"] = username
            target.sendall(b"+OK\r\n")
        else:
            # Reject with a standard Redis-compatible error message
            target.sendall(b"-WRONGPASS invalid username-password pair or user is disabled.\r\n")
            
        return True

    return False
