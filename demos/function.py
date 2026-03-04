import sys
import json
import time

def main(args):
    # 1. Check warmup
    if args.get("warmup", False):
        return {"warmup": True}

    # 2. Memory simulation
    memory_mb = args.get("memory", 0)
    if memory_mb > 0:
        try:
            # Allocate memory (approximate)
            # Python integers are objects, bytes are more efficient for raw allocation
            size_bytes = int(memory_mb * 1024 * 1024)
            # Allocation
            buffer = bytearray(size_bytes)
            # Touch to ensure allocation
            buffer[0] = 1
            buffer[-1] = 1
            # Explicitly delete to free (though function end will free it)
            del buffer
        except Exception as e:
            return {"error": str(e)}

    return {"status": "success", "memory_mb": memory_mb}

if __name__ == "__main__":
    # Local test
    print(main({"memory": 10}))
