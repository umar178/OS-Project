# ──────────────────────────────────────────────────────────────
#  Makefile — Local Storage Server
#  NUCES · Operating Systems Project · 2026
# ──────────────────────────────────────────────────────────────

CC      = gcc
CFLAGS  = -Wall -Wextra -g -O2 -pthread
LDFLAGS = -pthread -lrt

TARGETS = server client multi_client

.PHONY: all clean run-server run-multi help

all: $(TARGETS)

server: server.c common.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

client: client.c common.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

multi_client: multi_client.c common.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGETS)
	rm -rf storage/
	rm -f server.log
	# remove any leftover shared memory segments
	ipcs -m | awk '/0x/ {print $$2}' | xargs -r -I{} ipcrm -m {} 2>/dev/null || true

run-server: server
	./server

help:
	@echo ""
	@echo "  make            — build all binaries"
	@echo "  make clean      — remove binaries, storage/, logs, SHM"
	@echo "  make run-server — start the server"
	@echo ""
	@echo "  Then in other terminals:"
	@echo "    ./client <shmid>              # interactive client"
	@echo "    ./multi_client <shmid> [N]    # N concurrent clients (default 4)"
	@echo ""
