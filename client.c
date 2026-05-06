/*
 * client.c — Local Storage Server · Client
 * NUCES · Operating Systems Project · 2026
 *
 * Usage:  ./client <shmid>
 *
 * Each client instance:
 *   1. Attaches to the existing shared-memory segment.
 *   2. Claims a free slot (by PID).
 *   3. Presents an interactive menu.
 *   4. Sends requests via SHM semaphores and waits for responses.
 */

#include "common.h"

/* ── Globals ──────────────────────────────────────────────── */
static SharedMemory *shm      = NULL;
static int           my_slot  = -1;
static pid_t         my_pid;

/* ── Claim a free SHM slot ────────────────────────────────── */
static int claim_slot(void) {
    pthread_mutex_lock(&shm->shm_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (shm->slots[i].client_pid == 0) {
            shm->slots[i].client_pid = my_pid;
            pthread_mutex_unlock(&shm->shm_mutex);
            return i;
        }
    }
    pthread_mutex_unlock(&shm->shm_mutex);
    return -1; /* no free slot */
}

/* ── Release slot on exit ─────────────────────────────────── */
static void release_slot(void) {
    if (my_slot >= 0)
        shm->slots[my_slot].client_pid = 0;
}

/* ── Send a request and wait for response ─────────────────── */
static Status send_request(Operation op, const char *filename,
                            const char *data, int data_len) {
    SharedRequest *req = &shm->slots[my_slot];

    pthread_mutex_lock(&req->lock);
    req->operation    = op;
    req->data_len     = 0;
    req->response_ready = 0;

    if (filename) strncpy(req->filename, filename, MAX_FILENAME - 1);
    else          req->filename[0] = '\0';

    if (data && data_len > 0) {
        memcpy(req->data, data, data_len);
        req->data_len = data_len;
    } else {
        req->data[0] = '\0';
    }

    req->request_ready = 1;
    pthread_mutex_unlock(&req->lock);

    /* wake server */
    sem_post(&req->req_sem);

    /* wait for response (blocking) */
    sem_wait(&req->res_sem);

    return req->status;
}

/* ── Interactive menu helpers ─────────────────────────────── */
static void print_menu(void) {
    printf("\n┌─────────────────────────────────┐\n");
    printf("│  Local Storage Server — Client  │\n");
    printf("│  PID: %-5d   Slot: %-3d         │\n", my_pid, my_slot);
    printf("├─────────────────────────────────┤\n");
    printf("│  1. Upload file                 │\n");
    printf("│  2. Download file               │\n");
    printf("│  3. Delete file                 │\n");
    printf("│  4. List files                  │\n");
    printf("│  5. Exit                        │\n");
    printf("└─────────────────────────────────┘\n");
    printf("Choice: ");
}

static void do_upload(void) {
    char filename[MAX_FILENAME];
    char filepath[512];
    printf("Local file path to upload: ");
    if (!fgets(filepath, sizeof(filepath), stdin)) return;
    filepath[strcspn(filepath, "\n")] = '\0';

    printf("Store as (filename on server): ");
    if (!fgets(filename, sizeof(filename), stdin)) return;
    filename[strcspn(filename, "\n")] = '\0';

    /* read local file */
    int fd = open(filepath, O_RDONLY);
    if (fd < 0) {
        printf("[ERROR] Cannot open '%s': %s\n", filepath, strerror(errno));
        return;
    }
    char buf[MAX_DATA_SIZE];
    int  len = (int)read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (len < 0) { printf("[ERROR] Read failed\n"); return; }
    buf[len] = '\0';

    Status s = send_request(OP_UPLOAD, filename, buf, len);
    printf("[%s] %s\n", s == STATUS_OK ? "OK" : "FAIL",
           shm->slots[my_slot].message);
}

static void do_download(void) {
    char filename[MAX_FILENAME];
    printf("Filename to download: ");
    if (!fgets(filename, sizeof(filename), stdin)) return;
    filename[strcspn(filename, "\n")] = '\0';

    Status s = send_request(OP_DOWNLOAD, filename, NULL, 0);
    if (s == STATUS_OK) {
        printf("[OK] %s\n", shm->slots[my_slot].message);
        printf("─── Content ───────────────────────\n");
        printf("%.*s\n", shm->slots[my_slot].data_len,
               shm->slots[my_slot].data);
        printf("───────────────────────────────────\n");

        /* optionally save locally */
        printf("Save to local file? (leave blank to skip): ");
        char savepath[512];
        if (!fgets(savepath, sizeof(savepath), stdin)) return;
        savepath[strcspn(savepath, "\n")] = '\0';
        if (strlen(savepath) > 0) {
            int fd = open(savepath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd >= 0) {
                write(fd, shm->slots[my_slot].data,
                      shm->slots[my_slot].data_len);
                close(fd);
                printf("[OK] Saved to %s\n", savepath);
            } else {
                printf("[ERROR] %s\n", strerror(errno));
            }
        }
    } else {
        printf("[FAIL] %s\n", shm->slots[my_slot].message);
    }
}

static void do_delete(void) {
    char filename[MAX_FILENAME];
    printf("Filename to delete: ");
    if (!fgets(filename, sizeof(filename), stdin)) return;
    filename[strcspn(filename, "\n")] = '\0';

    printf("Confirm delete '%s'? [y/N]: ", filename);
    char confirm[8];
    if (!fgets(confirm, sizeof(confirm), stdin)) return;
    if (tolower(confirm[0]) != 'y') { printf("Cancelled.\n"); return; }

    Status s = send_request(OP_DELETE, filename, NULL, 0);
    printf("[%s] %s\n", s == STATUS_OK ? "OK" : "FAIL",
           shm->slots[my_slot].message);
}

static void do_list(void) {
    Status s = send_request(OP_LIST, NULL, NULL, 0);
    if (s == STATUS_OK) {
        printf("─── Files on server ────────────────\n");
        if (shm->slots[my_slot].data_len == 0)
            printf("  (empty)\n");
        else
            printf("%s", shm->slots[my_slot].data);
        printf("────────────────────────────────────\n");
    } else {
        printf("[FAIL] %s\n", shm->slots[my_slot].message);
    }
}

/* ── Entry point ──────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <shmid>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int shmid = atoi(argv[1]);
    my_pid    = getpid();

    /* attach to server's shared memory */
    shm = (SharedMemory *)shmat(shmid, NULL, 0);
    if (shm == (void *)-1) {
        perror("shmat");
        fprintf(stderr, "Is the server running? shmid=%d\n", shmid);
        exit(EXIT_FAILURE);
    }

    my_slot = claim_slot();
    if (my_slot < 0) {
        fprintf(stderr, "Server is full (all %d slots occupied).\n",
                MAX_CLIENTS);
        shmdt(shm);
        exit(EXIT_FAILURE);
    }

    printf("Connected to server (shmid=%d, slot=%d, pid=%d)\n",
           shmid, my_slot, my_pid);
    atexit(release_slot);

    int choice;
    while (1) {
        print_menu();
        if (scanf("%d", &choice) != 1) { /* eat bad input */
            int c; while ((c = getchar()) != '\n' && c != EOF);
            continue;
        }
        getchar(); /* consume newline */

        switch (choice) {
            case 1: do_upload();   break;
            case 2: do_download(); break;
            case 3: do_delete();   break;
            case 4: do_list();     break;
            case 5:
                send_request(OP_EXIT, NULL, NULL, 0);
                printf("Disconnected.\n");
                shmdt(shm);
                exit(EXIT_SUCCESS);
            default:
                printf("Invalid option.\n");
        }
    }
}
