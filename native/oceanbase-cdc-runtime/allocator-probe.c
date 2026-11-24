/* Diagnostic only: success does not establish native CDC compatibility. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/auxv.h>
#include <unistd.h>

int main(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <libobcdc.so> <expected kernel page bytes>\n", argv[0]);
        return 2;
    }
    char *end = NULL;
    errno = 0;
    unsigned long expected = strtoul(argv[2], &end, 10);
    if (errno || end == argv[2] || *end || !expected) return 2;
    unsigned long kernel_page = getauxval(AT_PAGESZ);
    long libc_page = sysconf(_SC_PAGESIZE);
    printf("AT_PAGESZ=%lu sysconf=%ld expected=%lu\n", kernel_page, libc_page, expected);
    fflush(stdout);
    if (kernel_page != expected || libc_page != (long) expected) {
        fprintf(stderr, "Wrong kernel page size; refusing a misleading compatibility result\n");
        return 2;
    }
    void *lib = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
    if (!lib) {
        fprintf(stderr, "dlopen failed: %s\n", dlerror());
        return 2;
    }
    int (*ctl)(const char *, void *, size_t *, void *, size_t) = dlsym(lib, "_rjem_mallctl");
    void *(*alloc)(size_t) = dlsym(lib, "_rjem_malloc");
    void (*release)(void *) = dlsym(lib, "_rjem_free");
    if (!ctl || !alloc || !release) {
        fprintf(stderr, "Vendor allocator symbols missing; result is inconclusive\n");
        return 2;
    }
    size_t page = 0, length = sizeof(page);
    int ret = ctl("arenas.page", &page, &length, NULL, 0);
    printf("_rjem_mallctl(arenas.page): ret=%d page=%zu\n", ret, page);
    fflush(stdout);
    if (ret) return 1;
    const size_t sizes[] = {1, 65536, 2097152};
    for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); ++i) {
        unsigned char *memory = alloc(sizes[i]);
        if (!memory) {
            fprintf(stderr, "Vendor allocator failed for %zu bytes\n", sizes[i]);
            return 1;
        }
        memset(memory, 0x5a, sizes[i]);
        release(memory);
    }
    puts("Allocator probe passed; native CDC initialization and DML verification are still required");
    return 0;
}
