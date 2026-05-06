#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include <dirent.h>
#include <stdarg.h>
#include <ctype.h>

/* ── Tunables ─────────────────────────────────────────────── */
#define MAX_CLIENTS       10
#define THREAD_POOL_SIZE  4
#define SHM_SIZE          4096          /* bytes per request slot  */
#define MAX_FILENAME      256
#define MAX_DATA_SIZE     2048
#define STORAGE_DIR       "./storage"
#define LOG_FILE          "./server.log"
#define SHM_KEY_PATH      "/tmp"
#define SHM_KEY_ID        42

/* ── Operation codes ──────────────────────────────────────── */
typedef enum {
    OP_UPLOAD   = 1,
    OP_DOWNLOAD = 2,
    OP_DELETE   = 3,
    OP_LIST     = 4,
    OP_EXIT     = 5
} Operation;

/* ── Status codes ─────────────────────────────────────────── */
typedef enum {
    STATUS_OK        =  0,
    STATUS_ERROR     = -1,
    STATUS_NOT_FOUND = -2,
    STATUS_EXISTS    = -3
} Status;

/* ── Shared-memory request/response structure ─────────────── */
typedef struct {
    /* synchronisation flags */
    volatile int  request_ready;   /* client sets 1, server clears */
    volatile int  response_ready;  /* server sets 1, client clears */

    pid_t         client_pid;
    Operation     operation;
    char          filename[MAX_FILENAME];
    char          data[MAX_DATA_SIZE];
    int           data_len;
    Status        status;
    char          message[512];

    sem_t         req_sem;         /* client → server signal       */
    sem_t         res_sem;         /* server → client signal       */
    pthread_mutex_t lock;
} SharedRequest;

/* ── Shared-memory control block (slot array) ─────────────── */
typedef struct {
    SharedRequest slots[MAX_CLIENTS];
    int           slot_count;
    pthread_mutex_t shm_mutex;
} SharedMemory;

/* ── Utility: get current timestamp string ────────────────── */
static inline void timestamp(char *buf, size_t sz) {
    time_t t = time(NULL);
    struct tm *tm_info = localtime(&t);
    strftime(buf, sz, "%Y-%m-%d %H:%M:%S", tm_info);
}

#endif /* COMMON_H */
