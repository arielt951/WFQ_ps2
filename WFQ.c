/*
 * Original WFQ Implementation with Simple Debug Output
 *
 * After each packet output, prints:
 * DEBUG: arrival_time=X, weight=Y, virtual_time=Z
 */

#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>

#define MAX_LINE_LEN     256
#define INITIAL_HEAP_CAP 1024
#define HASH_SIZE        1024
#define LINK_RATE        1.0  // 1 byte per time unit

 // Helper function for string duplication (cross-platform)
static char* my_strdup(const char* s) {
	size_t len = strlen(s) + 1;
	char* copy = malloc(len);
	if (copy) {
		memcpy(copy, s, len);
	}
	return copy;
}

//-------------------------------------------------------------------------
// Packet structure
//-------------------------------------------------------------------------
typedef struct Packet {
	double arrival_time;
	size_t length_bytes;
	double weight;
	double finish_vtime;    // GPS virtual finish time
	int flow_priority;      // For tie-breaking (order of first flow appearance)
	char *original_line;
} Packet;

//-------------------------------------------------------------------------
// Flow key: identifies a flow by its 4-tuple
//-------------------------------------------------------------------------
typedef struct {
	uint32_t src_ip, dst_ip;
	uint16_t src_port, dst_port;
} FlowKey;

//-------------------------------------------------------------------------
// Per-flow state
//-------------------------------------------------------------------------
typedef struct {
	double last_finish;       // finish tag of last packet in virtual time
	double weight;            // current flow weight
	int    backlog_count;     // number of packets enqueued
	int    priority;          // flow priority (order of first appearance)
	int    in_heap;
} FlowState;

//-------------------------------------------------------------------------
// Hash table entry for flow states
//-------------------------------------------------------------------------
typedef struct FlowEntry {
	FlowKey key;
	FlowState state;
	struct FlowEntry *next;
} FlowEntry;
static FlowEntry *flow_table[HASH_SIZE] = { 0 };

//-------------------------------------------------------------------------
// Global state
//-------------------------------------------------------------------------
double current_time = 0.0;      // Current real time
double V = 0.0;                  // Virtual time
double W_active = 0.0;           // Sum of weights of backlogged flows
int next_flow_priority = 0;      // For assigning flow priorities

//-------------------------------------------------------------------------
// Min-heap for pending packets (ordered by finish virtual time)
//-------------------------------------------------------------------------
static Packet **heap = NULL;
static size_t heap_size = 0, heap_cap = 0;

//-------------------------------------------------------------------------
// Function prototypes (to fix declaration order issues)
//-------------------------------------------------------------------------
static FlowKey extract_flow_key(const char *line);

//-------------------------------------------------------------------------
// Utility functions
//-------------------------------------------------------------------------
static uint32_t ip_to_uint(const char *s) {
	int a, b, c, d;
	if (sscanf(s, "%d.%d.%d.%d", &a, &b, &c, &d) != 4) return 0;
	return ((uint32_t)a << 24) | ((uint32_t)b << 16) | ((uint32_t)c << 8) | (uint32_t)d;
}

static unsigned long flow_hash(const FlowKey *k) {
	uint64_t h = ((uint64_t)k->src_ip << 32) ^ k->dst_ip
		^ ((uint32_t)k->src_port << 16) ^ k->dst_port;
	return (unsigned long)(h % HASH_SIZE);
}


static int flow_eq(const FlowKey *a, const FlowKey *b) {
	return a->src_ip == b->src_ip && a->dst_ip == b->dst_ip
		&& a->src_port == b->src_port && a->dst_port == b->dst_port;
}

//-------------------------------------------------------------------------
// Flow state management
//-------------------------------------------------------------------------
static FlowState *get_flow_state(const FlowKey *key) {
	unsigned long idx = flow_hash(key);
	for (FlowEntry *e = flow_table[idx]; e; e = e->next) {
		if (flow_eq(&e->key, key)) return &e->state;
	}
	FlowEntry *e = calloc(1, sizeof(*e));  // Use calloc to zero-initialize
	if (!e) { perror("calloc"); exit(EXIT_FAILURE); }
	e->key = *key;
	e->state.last_finish = 0.0;
	e->state.weight = 1.0;
	e->state.backlog_count = 0;
	e->state.priority = next_flow_priority++;
	e->next = flow_table[idx];
	flow_table[idx] = e;
	e->state.in_heap = 0;

	return &e->state;
}

//-------------------------------------------------------------------------
// Heap operations with proper tie-breaking for WFQ
//-------------------------------------------------------------------------
static void heap_swap(Packet **a, Packet **b) {
	Packet *tmp = *a; *a = *b; *b = tmp;
}

static int packet_compare(const Packet *a, const Packet *b) {
	// Primary: finish virtual time (most important for WFQ)
	if (a->finish_vtime < b->finish_vtime) return -1;
	if (a->finish_vtime > b->finish_vtime) return 1;

	// Secondary: flow priority (for same finish times, earlier flows win)
	if (a->flow_priority < b->flow_priority) return -1;
	if (a->flow_priority > b->flow_priority) return 1;

	// Tertiary: arrival time (earlier arrivals win)
	if (a->arrival_time < b->arrival_time) return -1;
	if (a->arrival_time > b->arrival_time) return 1;

	return 0;
}

static void heapify_up(size_t i) {
	while (i > 0) {
		size_t p = (i - 1) / 2;
		if (packet_compare(heap[i], heap[p]) < 0) {
			heap_swap(&heap[i], &heap[p]);
			i = p;
		}
		else break;
	}
}

static void heapify_down(size_t i) {
	for (;;) {
		size_t l = 2 * i + 1, r = 2 * i + 2, smallest = i;

		if (l < heap_size && packet_compare(heap[l], heap[smallest]) < 0) {
			smallest = l;
		}

		if (r < heap_size && packet_compare(heap[r], heap[smallest]) < 0) {
			smallest = r;
		}

		if (smallest != i) {
			heap_swap(&heap[i], &heap[smallest]);
			i = smallest;
		}
		else break;
	}
}

static void heap_push(Packet *p) {
	if (heap_size >= heap_cap) {
		heap_cap = heap_cap ? heap_cap * 2 : INITIAL_HEAP_CAP;
		heap = realloc(heap, heap_cap * sizeof(*heap));
		if (!heap) { perror("realloc"); exit(EXIT_FAILURE); }
	}
	heap[heap_size] = p;
	heapify_up(heap_size++);
}

// Rebuilds the heap to restore the min-heap property
static void heap_rebuild(void) {
	if (heap_size == 0) return;

	// Start from the last parent node and heapify down
	for (size_t i = (heap_size / 2); i-- > 0;) {
		heapify_down(i);
	}
}



// Checks whether the min-heap property is preserved; if not, fixes the heap
static void heap_validate_and_fix(void) {
	int issues = 0;
	for (size_t i = 0; i < heap_size; i++) {
		size_t l = 2 * i + 1, r = 2 * i + 2;

		if (l < heap_size && packet_compare(heap[l], heap[i]) < 0) {
			fprintf(stderr, "Heap violation: parent[%zu] > left[%zu]\n", i, l);
			issues = 1;
		}
		if (r < heap_size && packet_compare(heap[r], heap[i]) < 0) {
			fprintf(stderr, "Heap violation: parent[%zu] > right[%zu]\n", i, r);
			issues = 1;
		}
	}
	if (issues) {
		fprintf(stderr, "Heap violations found — fixing heap...\n");
		heap_rebuild();
	}
	else {
		fprintf(stderr, "Heap is valid.\n");
	}
}



static Packet *heap_pop(void) {
	if (!heap_size) return NULL;
	Packet *top = heap[0];
	heap[0] = heap[--heap_size];
	if (heap_size > 0) heapify_down(0);

	return top;
}

//-------------------------------------------------------------------------
// Extract flow key from packet line
//-------------------------------------------------------------------------
static FlowKey extract_flow_key(const char *line) {
	char src_str[16], dst_str[16];
	int sp, dp;
	sscanf(line, "%*lf %15s %d %15s %d", src_str, &sp, dst_str, &dp);

	FlowKey key;
	key.src_ip = ip_to_uint(src_str);
	key.dst_ip = ip_to_uint(dst_str);
	key.src_port = (uint16_t)sp;
	key.dst_port = (uint16_t)dp;
	return key;
}

// Recalculate finish_vtime for all packets in the heap using latest V and flow state
static void recalculate_all_finish_vtimes(void) {
	for (size_t i = 0; i < heap_size; i++) {
		Packet *p = heap[i];
		FlowKey key = extract_flow_key(p->original_line);
		FlowState *fs = get_flow_state(&key);

		double start_vtime = (fs->last_finish > V) ? fs->last_finish : V;
		p->finish_vtime = start_vtime + (double)p->length_bytes / fs->weight;
	}
	heap_rebuild();  // Reorder heap based on new finish_vtime values
}


//-------------------------------------------------------------------------
// Update virtual time based on elapsed real time and active weight
//-------------------------------------------------------------------------
static void advance_virtual_time_to(double target_time) {
	if (target_time <= current_time || W_active <= 0.0) return;

	// Ensure we advance from the current round(t), not a possibly stale V
	double elapsed = target_time - current_time;
	V = V + elapsed / W_active;  // round(t + x) = round(t) + x/W
	current_time = target_time;
}


// For departures: update both virtual time and real time
static void advance_time_to(double new_time) {
	if (new_time > current_time) {
		if (W_active > 0.0) {
			V += (new_time - current_time) / W_active;
		}
		current_time = new_time;
	}
}

//-------------------------------------------------------------------------
// Debug function to show heap contents
//-------------------------------------------------------------------------
static void debug_heap_contents(const char* when) {
	printf("HEAP_DEBUG %s (time=%.0f, heap_size=%d):\n", when, current_time, (int)heap_size);
	for (int i = 0; i < (int)heap_size && i < 10; i++) {  // Show top 10 packets
		printf("  [%d] arrival=%.0f, finish_vtime=%.6f, priority=%d\n",
			i, heap[i]->arrival_time, heap[i]->finish_vtime, heap[i]->flow_priority);
	}
}

//-------------------------------------------------------------------------
// Input parsing
//-------------------------------------------------------------------------
static Packet *read_one_packet(void) {
	char line[MAX_LINE_LEN];
	while (fgets(line, sizeof(line), stdin)) {
		size_t len = strlen(line);
		if (len <= 1) continue;  // Skip empty lines
		if (line[len - 1] == '\n') line[len - 1] = '\0';

		double t, w_in = 1.0;
		char src[16], dst[16];
		int sp, dp;
		size_t plen;

		int n = sscanf(line, "%lf %15s %d %15s %d %zu %lf",
			&t, src, &sp, dst, &dp, &plen, &w_in);

		if (n < 6) continue;  // Skip malformed lines

		Packet *p = calloc(1, sizeof(*p));  // Use calloc to zero-initialize
		if (!p) { perror("calloc"); exit(EXIT_FAILURE); }

		p->arrival_time = t;
		p->length_bytes = plen;
		p->weight = (n >= 7) ? w_in : 1.0;
		p->flow_priority = 0;  // Explicitly initialize
		p->finish_vtime = 0.0; // Explicitly initialize
		p->original_line = my_strdup(line);

		return p;
	}
	return NULL;
}

//-------------------------------------------------------------------------
// Main simulation
//-------------------------------------------------------------------------
int main(void) {
	int debugx = 0;
	current_time = 0.0;
	V = 0.0;
	W_active = 0.0;
	next_flow_priority = 0;

	Packet *next_arrival = read_one_packet();
	//printf("packet content %.0f\n", next_arrival->arrival_time);

	while (next_arrival || heap_size > 0) {

		double next_arrival_time = next_arrival ? next_arrival->arrival_time : 1e9; //check if there are any new arrivals, if not handle departures from heap

		// Decide whether to process arrival or departure
		// Find next possible departure time
		double next_departure_time = 1e9;

		if (heap_size > 0) {
			//heap_validate_and_fix();
			next_departure_time = (current_time > heap[0]->arrival_time) ? //handles case where the packet was delayed and is transmitted in later time then in the input
				current_time : heap[0]->arrival_time;
		}

		if (next_arrival &&
			(heap_size == 0 || next_arrival_time < next_departure_time)) {

			// === ARRIVAL EVENT ===
			double arrival_time = next_arrival_time;

			// DEBUG: Track specific packets we're interested in
			//if (arrival_time == 47971 || arrival_time == 48283) {
			//	printf("TRACK: Packet %.0f ARRIVING\n", arrival_time);
		//	}

			// Extract flow information FIRST
			FlowKey key = extract_flow_key(next_arrival->original_line);
			FlowState *fs = get_flow_state(&key);

			// Store the virtual time BEFORE we advance it
			double gps_arrival_vtime = V;

			// Now update virtual time to arrival time
			advance_virtual_time_to(arrival_time);

			// Add flow to active set if it wasn't backlogged
			if (fs->backlog_count == 0) {
				W_active += fs->weight;
			}

			// Increment backlog count
			fs->backlog_count++;

			// Update flow weight if packet specifies one (affects FUTURE packets)
			if (next_arrival->weight != 1.0) {
				// Adjust W_active for the weight change
				W_active = W_active - fs->weight + next_arrival->weight;
				fs->weight = next_arrival->weight;
			}

			// Calculate GPS virtual finish time
			next_arrival->flow_priority = fs->priority;

			/*double start_vtime = (fs->last_finish > gps_arrival_vtime) ? fs->last_finish : gps_arrival_vtime;
			next_arrival->finish_vtime = start_vtime + (double)next_arrival->length_bytes / fs->weight;
			fs->last_finish = next_arrival->finish_vtime;*/

			double start_vtime = (fs->last_finish > gps_arrival_vtime) ? fs->last_finish : gps_arrival_vtime;
			next_arrival->finish_vtime = start_vtime + (double)next_arrival->length_bytes / fs->weight;
			fs->last_finish = next_arrival->finish_vtime;
			// Add to heap (will be sorted by finish time with proper tie-breaking)
			//heap_push(next_arrival);
			if (!fs->in_heap) {
				heap_push(next_arrival);
				fs->in_heap = 1;
				
			}



			// Show heap contents around problem time

			// Read next packet
			next_arrival = read_one_packet();
			//debug_heap_contents("after_arrival");

			//printf("packet content %.0f\n", next_arrival->arrival_time);

		}
		else if (heap_size > 0) {
			// === DEPARTURE EVENT ===

			// Show heap contents before selecting packet
			if (debugx == 1) {
				debug_heap_contents("before_recalculation");
			}

			recalculate_all_finish_vtimes();

			//debug_heap_contents("before_departure");

			// Get packet with earliest finish time (WFQ scheduling)
			Packet *departing = heap_pop();


			// Ensure we don't start transmission before packet arrives
			if (current_time < departing->arrival_time) {
				advance_time_to(departing->arrival_time);
			}

			// Output packet with its transmission start time
			printf("%d: %s\n", (int)round(current_time), departing->original_line);
			//printf("DEBUG: arrival_time=%.0f, weight=%.2f, virtual_time=%.6f\n",
				//departing->arrival_time, departing->weight, V);

			// Advance current time by transmission duration
			current_time += (double)departing->length_bytes / LINK_RATE;

			// Update flow state
			FlowKey key = extract_flow_key(departing->original_line);
			FlowState *fs = get_flow_state(&key);
			fs->backlog_count--;

			// Check if flow becomes inactive
			if (fs->backlog_count == 0) {
				W_active -= fs->weight;
			}
			fs->in_heap = 0;
			// Cleanup
			free(departing->original_line);
			free(departing);
		}
	}

	// Cleanup
	free(heap);

	for (int i = 0; i < HASH_SIZE; i++) {
		FlowEntry *e = flow_table[i];
		while (e) {
			FlowEntry *next = e->next;
			free(e);
			e = next;
		}
	}

	return 0;
}