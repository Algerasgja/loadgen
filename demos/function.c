#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Simple check for substring since we don't want to link full JSON parser
int contains_key_true(const char *json, const char *key) {
    char pattern[256];
    sprintf(pattern, "\"%s\":true", key);
    if (strstr(json, pattern)) return 1;
    sprintf(pattern, "\"%s\": true", key);
    if (strstr(json, pattern)) return 1;
    return 0;
}

int get_int_value(const char *json, const char *key) {
    char pattern[256];
    sprintf(pattern, "\"%s\":", key);
    char *p = strstr(json, pattern);
    if (!p) return -1;
    
    p += strlen(pattern);
    while (*p == ' ' || *p == '\t') p++;
    
    return atoi(p);
}

int main(int argc, char *argv[]) {
    // OpenWhisk native action receives params as the last argument
    const char *params = (argc > 0) ? argv[argc - 1] : "{}";

    // 1. Check warmup
    if (contains_key_true(params, "warmup")) {
        // Warmup: exit immediately
        printf("{\"warmup\": true}");
        return 0;
    }

    // 2. Memory simulation
    int memory_mb = get_int_value(params, "memory");
    if (memory_mb > 0) {
        size_t size = memory_mb * 1024 * 1024;
        char *buffer = malloc(size);
        if (buffer) {
            memset(buffer, 1, size); // Touch memory to ensure allocation
            // Hold for a bit? User didn't specify duration, just memory.
            // We assume short duration or controlled by other means.
            // To prevent optimization out, use the buffer.
            buffer[0] = 0;
            free(buffer);
        }
    }

    // Return simple result
    printf("{\"status\": \"success\", \"memory_mb\": %d}", memory_mb);
    return 0;
}
