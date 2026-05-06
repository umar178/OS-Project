/*
 * server.c — Local Storage Server
 * NUCES · Operating Systems Project · 2026
 *
 * Architecture
 *   main()  → creates shared memory, initialises thread pool,
 *             then loops accepting client connections via SHM slots.
 *   Each worker thread blocks on a semaphore and processes one
 *   request at a time, guarded by mutexes for FS and log writes.
 */

#include "common.h"

/* ── Globals ──────────────────────────────────────────────── */
static SharedMemory  *shm    = NULL;
static int            shmid  = -1;
static FILE          *logfp  = NULL;
static pthread_mutex_t log_mutex   = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t fs_mutex    = PTHREAD_MUTEX_INITIALIZER;

/* Thread-pool work queue */
typedef struct WorkItem {
    int slot_index;
    struct WorkItem *next;
} WorkItem;

static WorkItem        *queue_head = NULL;
static WorkItem        *queue_tail = NULL;
static pthread_mutex_t  queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t   queue_cond  = PTHREAD_COND_INITIALIZER;
static int              server_running = 1;

/* ── Logging ──────────────────────────────────────────────── */
static void log_event(const char *level, const char *fmt, ...) {
    char ts[32];
    timestamp(ts, sizeof(ts));

    pthread_mutex_lock(&log_mutex);
    va_list ap;
    va_start(ap, fmt);
    if (logfp) {
        fprintf(logfp, "[%s] [%s] ", ts, level);
        vfprintf(logfp, fmt, ap);
        fprintf(logfp, "\n");
        fflush(logfp);
    }
    /* mirror to stdout */
    printf("[%s] [%s] ", ts, level);
    vfprintf(stdout, fmt, ap);
    printf("\n");
    va_end(ap);
    pthread_mutex_unlock(&log_mutex);
}

/* ── File-system helpers ──────────────────────────────────── */
static void ensure_storage_dir(void) {
    struct stat st = {0};
    if (stat(STORAGE_DIR, &st) == -1) {
        if (mkdir(STORAGE_DIR, 0755) == 0)
            log_event("INFO", "Created storage directory: %s", STORAGE_DIR);
        else {
            perror("mkdir");
            exit(EXIT_FAILURE);
        }
    }
}

/* UPLOAD: write data to STORAGE_DIR/filename */
static Status handle_upload(const char *filename, const char *data,
                             int data_len, char *msg) {
    if (strpbrk(filename, "/\\..")) {
        snprintf(msg, 512, "Invalid filename: %s", filename);
        return STATUS_ERROR;
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", STORAGE_DIR, filename);

    pthread_mutex_lock(&fs_mutex);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        pthread_mutex_unlock(&fs_mutex);
        snprintf(msg, 512, "Cannot create file: %s", strerror(errno));
        return STATUS_ERROR;
    }
    ssize_t written = write(fd, data, data_len);
    close(fd);
    pthread_mutex_unlock(&fs_mutex);

    if (written != data_len) {
        snprintf(msg, 512, "Partial write (%zd/%d bytes)", written, data_len);
        return STATUS_ERROR;
    }
    snprintf(msg, 512, "Uploaded %s (%d bytes)", filename, data_len);
    log_event("INFO", "UPLOAD   pid=%-6d file=%-30s bytes=%d",
              getpid(), filename, data_len);
    return STATUS_OK;
}

/* DOWNLOAD: read STORAGE_DIR/filename into data buffer */
static Status handle_download(const char *filename, char *data,
                               int *data_len, char *msg) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", STORAGE_DIR, filename);

    pthread_mutex_lock(&fs_mutex);
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        pthread_mutex_unlock(&fs_mutex);
        snprintf(msg, 512, "File not found: %s", filename);
        return STATUS_NOT_FOUND;
    }
    *data_len = (int)read(fd, data, MAX_DATA_SIZE - 1);
    close(fd);
    pthread_mutex_unlock(&fs_mutex);

    if (*data_len < 0) {
        snprintf(msg, 512, "Read error: %s", strerror(errno));
        return STATUS_ERROR;
    }
    data[*data_len] = '\0';
    snprintf(msg, 512, "Downloaded %s (%d bytes)", filename, *data_len);
    log_event("INFO", "DOWNLOAD pid=%-6d file=%-30s bytes=%d",
              getpid(), filename, *data_len);
    return STATUS_OK;
}

/* DELETE */
static Status handle_delete(const char *filename, char *msg) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", STORAGE_DIR, filename);

    pthread_mutex_lock(&fs_mutex);
    int rc = remove(path);
    pthread_mutex_unlock(&fs_mutex);

    if (rc != 0) {
        snprintf(msg, 512, "Delete failed: %s", strerror(errno));
        return (errno == ENOENT) ? STATUS_NOT_FOUND : STATUS_ERROR;
    }
    snprintf(msg, 512, "Deleted %s", filename);
    log_event("INFO", "DELETE   file=%s", filename);
    return STATUS_OK;
}

/* LIST: enumerate STORAGE_DIR, pack names separated by '\n' */
static Status handle_list(char *data, int *data_len, char *msg) {
    pthread_mutex_lock(&fs_mutex);
    DIR *dir = opendir(STORAGE_DIR);
    if (!dir) {
        pthread_mutex_unlock(&fs_mutex);
        snprintf(msg, 512, "Cannot open storage directory");
        return STATUS_ERROR;
    }
    data[0] = '\0';
    struct dirent *entry;
    int total = 0;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] == '.') continue;
        strncat(data, entry->d_name, MAX_DATA_SIZE - total - 2);
        strcat(data, "\n");
        total += (int)strlen(entry->d_name) + 1;
        if (total >= MAX_DATA_SIZE - 64) break;
    }
    closedir(dir);
    pthread_mutex_unlock(&fs_mutex);

    *data_len = (int)strlen(data);
    snprintf(msg, 512, "Listed %d file(s)", (*data_len > 0));
    log_event("INFO", "LIST     %d bytes of listing returned", *data_len);
    return STATUS_OK;
}

/* ── Request dispatcher (runs in worker thread) ───────────── */
static void dispatch_request(int idx) {
    SharedRequest *req = &shm->slots[idx];

    switch (req->operation) {
        case OP_UPLOAD:
            req->status = handle_upload(req->filename, req->data,
                                        req->data_len, req->message);
            break;
        case OP_DOWNLOAD:
            req->status = handle_download(req->filename, req->data,
                                          &req->data_len, req->message);
            break;
        case OP_DELETE:
            req->status = handle_delete(req->filename, req->message);
            break;
        case OP_LIST:
            req->status = handle_list(req->data, &req->data_len,
                                      req->message);
            break;
        case OP_EXIT:
            snprintf(req->message, 512, "Goodbye, client %d", req->client_pid);
            req->status = STATUS_OK;
            log_event("INFO", "Client pid=%d disconnected", req->client_pid);
            break;
        default:
            req->status = STATUS_ERROR;
            snprintf(req->message, 512, "Unknown operation %d", req->operation);
    }
}

/* ── Worker thread function ───────────────────────────────── */
static void *worker_thread(void *arg) {
    int tid = *(int *)arg;
    free(arg);
    log_event("INFO", "Worker thread %d started (tid=%lu)", tid,
              (unsigned long)pthread_self());

    while (1) {
        pthread_mutex_lock(&queue_mutex);
        while (queue_head == NULL && server_running)
            pthread_cond_wait(&queue_cond, &queue_mutex);

        if (!server_running && queue_head == NULL) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }

        WorkItem *item = queue_head;
        queue_head = queue_head->next;
        if (queue_head == NULL) queue_tail = NULL;
        pthread_mutex_unlock(&queue_mutex);

        int idx = item->slot_index;
        free(item);

        log_event("INFO", "Thread %d handling slot %d (op=%d, pid=%d)",
                  tid, idx, shm->slots[idx].operation, shm->slots[idx].client_pid);

        dispatch_request(idx);

        /* signal client that response is ready */
        shm->slots[idx].response_ready = 1;
        sem_post(&shm->slots[idx].res_sem);
    }

    log_event("INFO", "Worker thread %d exiting", tid);
    return NULL;
}

/* ── Main server loop ─────────────────────────────────────── */
static void server_loop(void) {
    log_event("INFO", "Server loop started — watching %d slots", MAX_CLIENTS);

    while (server_running) {
        for (int i = 0; i < MAX_CLIENTS; i++) {
            SharedRequest *req = &shm->slots[i];

            /* non-blocking: try to decrement semaphore */
            if (sem_trywait(&req->req_sem) == 0) {
                if (!req->request_ready) continue;
                req->request_ready = 0;

                /* enqueue to thread pool */
                WorkItem *item = malloc(sizeof(WorkItem));
                item->slot_index = i;
                item->next = NULL;

                pthread_mutex_lock(&queue_mutex);
                if (queue_tail)
                    queue_tail->next = item;
                else
                    queue_head = item;
                queue_tail = item;
                pthread_cond_signal(&queue_cond);
                pthread_mutex_unlock(&queue_mutex);
            }
        }
        usleep(1000); /* 1 ms poll interval */
    }
}

/* ── Cleanup ──────────────────────────────────────────────── */
static void cleanup(void) {
    log_event("INFO", "Server shutting down …");

    /* stop workers */
    pthread_mutex_lock(&queue_mutex);
    server_running = 0;
    pthread_cond_broadcast(&queue_cond);
    pthread_mutex_unlock(&queue_mutex);

    /* destroy SHM semaphores & mutexes */
    for (int i = 0; i < MAX_CLIENTS; i++) {
        sem_destroy(&shm->slots[i].req_sem);
        sem_destroy(&shm->slots[i].res_sem);
        pthread_mutex_destroy(&shm->slots[i].lock);
    }
    pthread_mutex_destroy(&shm->shm_mutex);

    /* detach & remove shared memory */
    shmdt(shm);
    shmctl(shmid, IPC_RMID, NULL);

    if (logfp) fclose(logfp);
    log_event("INFO", "Cleanup complete. Bye!");
}

/* ── Entry point ──────────────────────────────────────────── */
int main(void) {
    printf("╔══════════════════════════════════════╗\n");
    printf("║   Local Storage Server  (NUCES 2026) ║\n");
    printf("╚══════════════════════════════════════╝\n\n");

    /* open log */
    logfp = fopen(LOG_FILE, "a");
    if (!logfp) { perror("fopen log"); exit(EXIT_FAILURE); }

    ensure_storage_dir();

    /* create / attach shared memory */
    key_t key = ftok(SHM_KEY_PATH, SHM_KEY_ID);
    shmid = shmget(key, sizeof(SharedMemory), IPC_CREAT | IPC_EXCL | 0666);
    if (shmid < 0) {
        /* might already exist from a crashed run — remove and retry */
        shmid = shmget(key, sizeof(SharedMemory), 0666);
        if (shmid >= 0) shmctl(shmid, IPC_RMID, NULL);
        shmid = shmget(key, sizeof(SharedMemory), IPC_CREAT | 0666);
    }
    if (shmid < 0) { perror("shmget"); exit(EXIT_FAILURE); }

    shm = (SharedMemory *)shmat(shmid, NULL, 0);
    if (shm == (void *)-1) { perror("shmat"); exit(EXIT_FAILURE); }
    memset(shm, 0, sizeof(SharedMemory));

    /* initialise per-slot semaphores and mutexes */
    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);

    pthread_mutexattr_t shm_mattr;
    pthread_mutexattr_init(&shm_mattr);
    pthread_mutexattr_setpshared(&shm_mattr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&shm->shm_mutex, &shm_mattr);

    sem_t dummy; /* check process-shared semaphore support */
    (void)dummy;

    for (int i = 0; i < MAX_CLIENTS; i++) {
        shm->slots[i].request_ready  = 0;
        shm->slots[i].response_ready = 0;
        sem_init(&shm->slots[i].req_sem, 1, 0);
        sem_init(&shm->slots[i].res_sem, 1, 0);
        pthread_mutex_init(&shm->slots[i].lock, &mattr);
    }
    pthread_mutexattr_destroy(&mattr);

    log_event("INFO", "Shared memory initialised (shmid=%d, size=%zu bytes)",
              shmid, sizeof(SharedMemory));

    /* spawn thread pool */
    pthread_t threads[THREAD_POOL_SIZE];
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        int *tid = malloc(sizeof(int));
        *tid = i;
        if (pthread_create(&threads[i], NULL, worker_thread, tid) != 0) {
            log_event("ERROR", "Failed to create worker thread %d", i);
            exit(EXIT_FAILURE);
        }
    }
    log_event("INFO", "Thread pool ready (%d workers)", THREAD_POOL_SIZE);
    log_event("INFO", "Waiting for clients … (shmid=%d)", shmid);
    printf("\n[Server] shmid = %d  (pass this to clients)\n\n", shmid);

    server_loop();

    /* join workers */
    for (int i = 0; i < THREAD_POOL_SIZE; i++)
        pthread_join(threads[i], NULL);

    cleanup();
    return 0;
}
