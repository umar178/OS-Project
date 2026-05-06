/*
 * multi_client.c — Automated multi-client simulation
 * NUCES · Operating Systems Project · 2026
 *
 * Forks N child processes, each acting as a client.
 * Exercises UPLOAD / LIST / DOWNLOAD / DELETE concurrently
 * to demonstrate thread-pool and synchronisation correctness.
 *
 * Usage:  ./multi_client <shmid> [num_clients]
 *         num_clients defaults to 4
 */

#include "common.h"

static SharedMemory *shm     = NULL;
static int           my_slot = -1;
static pid_t         my_pid;

/* ── Claim / release helpers (same as client.c) ─────────────*/
static int claim_slot(void) {
    pthread_mutex_lock(&shm->shm_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (shm->slots[i].client_pid == 0) {
            shm->slots[i].client_pid = getpid();
            pthread_mutex_unlock(&shm->shm_mutex);
            return i;
        }
    }
    pthread_mutex_unlock(&shm->shm_mutex);
    return -1;
}

static void release_slot(void) {
    if (my_slot >= 0 && shm)
        shm->slots[my_slot].client_pid = 0;
}

static Status send_request(Operation op, const char *filename,
                            const char *data, int data_len) {
    SharedRequest *req = &shm->slots[my_slot];
    pthread_mutex_lock(&req->lock);
    req->operation    = op;
    req->data_len     = 0;
    req->response_ready = 0;
    if (filename) strncpy(req->filename, filename, MAX_FILENAME - 1);
    else          req->filename[0] = '\0';
    if (data && data_len > 0) { memcpy(req->data, data, data_len); req->data_len = data_len; }
    else req->data[0] = '\0';
    req->request_ready = 1;
    pthread_mutex_unlock(&req->lock);
    sem_post(&req->req_sem);
    sem_wait(&req->res_sem);
    return req->status;
}

/* ── Child worker: runs a sequence of operations ────────────*/
static void run_client_scenario(int client_id) {
    my_pid  = getpid();
    my_slot = claim_slot();
    if (my_slot < 0) {
        fprintf(stderr, "[Client %d] No free slot!\n", client_id);
        exit(EXIT_FAILURE);
    }
    atexit(release_slot);

    printf("[Client %2d | pid=%d | slot=%d] Started\n",
           client_id, my_pid, my_slot);

    /* small random jitter so clients don't all fire at once */
    usleep((rand() % 200) * 1000);

    /* 1. UPLOAD a unique test file */
    char fname[MAX_FILENAME];
    snprintf(fname, sizeof(fname), "client%02d_file.txt", client_id);

    char content[256];
    snprintf(content, sizeof(content),
             "Hello from client %d (pid=%d)! Timestamp: %ld",
             client_id, my_pid, (long)time(NULL));

    Status s = send_request(OP_UPLOAD, fname, content, (int)strlen(content));
    printf("[Client %2d] UPLOAD  %-25s → %s\n", client_id, fname,
           s == STATUS_OK ? "OK" : shm->slots[my_slot].message);

    usleep(50000); /* 50 ms */

    /* 2. LIST */
    s = send_request(OP_LIST, NULL, NULL, 0);
    printf("[Client %2d] LIST    → %s (%d bytes)\n", client_id,
           s == STATUS_OK ? "OK" : "FAIL", shm->slots[my_slot].data_len);

    usleep(50000);

    /* 3. DOWNLOAD the file just uploaded */
    s = send_request(OP_DOWNLOAD, fname, NULL, 0);
    printf("[Client %2d] DOWNLOAD %-24s → %s\n", client_id, fname,
           s == STATUS_OK ? "OK" : shm->slots[my_slot].message);

    usleep(50000);

    /* 4. DELETE */
    s = send_request(OP_DELETE, fname, NULL, 0);
    printf("[Client %2d] DELETE  %-25s → %s\n", client_id, fname,
           s == STATUS_OK ? "OK" : shm->slots[my_slot].message);

    /* 5. Graceful exit */
    send_request(OP_EXIT, NULL, NULL, 0);
    printf("[Client %2d] Done.\n", client_id);
    shmdt(shm);
    exit(EXIT_SUCCESS);
}

/* ── Entry point ─────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <shmid> [num_clients]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int shmid       = atoi(argv[1]);
    int num_clients = (argc >= 3) ? atoi(argv[2]) : 4;
    if (num_clients > MAX_CLIENTS) num_clients = MAX_CLIENTS;

    /* parent attaches SHM just to verify it exists */
    shm = (SharedMemory *)shmat(shmid, NULL, 0);
    if (shm == (void *)-1) {
        perror("shmat");
        exit(EXIT_FAILURE);
    }
    shmdt(shm);
    shm = NULL;

    printf("╔══════════════════════════════════════════╗\n");
    printf("║  Multi-Client Simulation  (%d clients)   ║\n", num_clients);
    printf("╚══════════════════════════════════════════╝\n\n");

    srand((unsigned)time(NULL));
    pid_t pids[MAX_CLIENTS];

    for (int i = 0; i < num_clients; i++) {
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); exit(EXIT_FAILURE); }
        if (pid == 0) {
            /* child: re-attach SHM and run scenario */
            shm = (SharedMemory *)shmat(shmid, NULL, 0);
            if (shm == (void *)-1) { perror("shmat child"); exit(EXIT_FAILURE); }
            run_client_scenario(i + 1);
            /* never returns */
        }
        pids[i] = pid;
    }

    /* parent waits for all children */
    printf("\n[Parent pid=%d] Waiting for %d child clients …\n\n",
           getpid(), num_clients);

    int all_ok = 1;
    for (int i = 0; i < num_clients; i++) {
        int status;
        waitpid(pids[i], &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            printf("[Parent] Client %d exited abnormally\n", i + 1);
            all_ok = 0;
        }
    }

    printf("\n[Parent] All clients finished. %s\n",
           all_ok ? "✓ All OK" : "⚠ Some failures — check server log.");
    return 0;
}
