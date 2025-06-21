#define _CRT_SECURE_NO_WARNINGS  // disable warning on strcpy/strtok, strdup
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <float.h>
#define MAX_IP_LEN 16
#define LINE_BUFFER_SZ 512
#define MAX_FLOWS 2048
#define BUF_INITIAL_CAP  64000
// struct to define a flow
typedef struct {
	char src_ip[MAX_IP_LEN];
	int src_port;
	char dst_ip[MAX_IP_LEN];
	int dst_port;
} Connection; 

//packet struck from each flow 
typedef struct {
	long long arrival_time;
	char src_ip[MAX_IP_LEN];
	int src_port;
	char dst_ip[MAX_IP_LEN];
	int dst_port;
	int length;
	double weight;
	int has_weight;
	char original_line[LINE_BUFFER_SZ];

	// WFQ specific fields
	double virtual_start_time;
	double virtual_finish_time;
	int connection_id;
	int appearance_order;
	char is_on_bus;
} Packet;

typedef struct {
	Connection flow;
	double weight;
	double virtual_finish_time;
	int appearance_order;
	int active;
} ConnectionInfo;

typedef struct {
	Packet *packets;
	int count;
	int capacity;
} PacketQueue;

// Global state
ConnectionInfo connections[MAX_FLOWS];
int num_connections = 0;
double virtual_time = 0.0;
double next_departure_time = 0; // Represents when the server becomes free next
PacketQueue pending_packets = { NULL, 0, 0 };
PacketQueue ready_queue = { NULL, 0, 0 };
PacketQueue virtual_bus = { NULL, 0, 0 };
double last_virtual_change = 0.0;
double current_time = 0.0;
char is_packet_on_bus = 0;
int packet_on_bus_idx = 0;
int should_remove_from_virtual_bus = 0;
long long next_virtual_end = 0;
int Debug = 0;

// Function prototypes
int find_or_create_connection(const char* src_ip, int src_port, const char* dst_ip, int dst_port, int appearance_order);
void parse_packet(const char* line, Packet* packet, int appearance_order);
void add_packet_to_queue(PacketQueue* queue, const Packet* packet);
void remove_packet_from_queue(PacketQueue* queue, int index);
int compare_packets_by_arrival_time(const void* a, const void* b);
void schedule_next_packet();
void init_packet_queue(PacketQueue* queue);
void cleanup();
char* my_strdup(const char* s);
double sum_Active_weights();

double sum_Active_weights() {
	int num_active_ids = 0;
	int active_conn_ids[MAX_FLOWS] = { 0 };
	double current_weight_sum = 0;
	for (int i = 0; i < virtual_bus.count; i++) {
		int conn_id = virtual_bus.packets[i].connection_id;
		int found = 0;
		for (int k = 0; k < num_active_ids; ++k) {
			if (active_conn_ids[k] == conn_id) {
				found = 1;
				break;
			}
		}
		if (!found) {
			current_weight_sum += virtual_bus.packets[i].weight;
			active_conn_ids[num_active_ids++] = conn_id;
		}
	}
	return current_weight_sum;
}
char* my_strdup(const char* s) {
	size_t len = strlen(s) + 1;
	char* copy = malloc(len);
	if (copy) {
		memcpy(copy, s, len);
	}
	return copy;
}

// Comparison function for qsort
int compare_packets_by_arrival_time(const void* a, const void* b) {
	const Packet* pa = (const Packet*)a;
	const Packet* pb = (const Packet*)b;
	if (pa->arrival_time < pb->arrival_time) return -1;
	if (pa->arrival_time > pb->arrival_time) return 1;
	return pa->appearance_order - pb->appearance_order;
}

int compare_packets_by_virtual_finish_time(const void* a, const void* b) {
	const Packet* pa = (const Packet*)a;
	const Packet* pb = (const Packet*)b;
	if (pa->virtual_finish_time < pb->virtual_finish_time) return -1;
	if (pa->virtual_finish_time > pb->virtual_finish_time) return 1;
	return pa->appearance_order - pb->appearance_order;
}

void init_packet_queue(PacketQueue* queue) {
	queue->capacity = BUF_INITIAL_CAP ;
	queue->packets = malloc(queue->capacity * sizeof(Packet));
	queue->count = 0;
}

int find_or_create_connection(const char* src_ip, int src_port, const char* dst_ip, int dst_port, int appearance_order) {
	// Look for existing connection
	for (int i = 0; i < num_connections; i++) {
		if (strcmp(connections[i].flow.src_ip, src_ip) == 0 &&
			connections[i].flow.src_port == src_port &&
			strcmp(connections[i].flow.dst_ip, dst_ip) == 0 &&
			connections[i].flow.dst_port == dst_port) {
			return i;
		}
	}

	// Create new connection
	if (num_connections >= MAX_FLOWS) {
		fprintf(stderr, "Too many connections\n");
		exit(1);
	}

	int id = num_connections++;
	strcpy(connections[id].flow.src_ip, src_ip);
	connections[id].flow.src_port = src_port;
	strcpy(connections[id].flow.dst_ip, dst_ip);
	connections[id].flow.dst_port = dst_port;
	connections[id].weight = 1; // Default weight - THIS MUST BE 1
	connections[id].virtual_finish_time = 0.0;
	connections[id].appearance_order = appearance_order;
	connections[id].active = 0;

	return id;
}

void parse_packet(const char* line, Packet* packet, int appearance_order) {
	strcpy(packet->original_line, line);
	packet->appearance_order = appearance_order;
	packet->has_weight = 0;
	packet->is_on_bus = 0;

	char* line_copy = my_strdup(line);
	char* token = strtok(line_copy, " ");
	int field = 0;

	while (token != NULL) {
		switch (field) {
		case 0: packet->arrival_time = atoll(token); break;
		case 1: strcpy(packet->src_ip, token); break;
		case 2: packet->src_port = atoi(token); break;
		case 3: strcpy(packet->dst_ip, token); break;
		case 4: packet->dst_port = atoi(token); break;
		case 5: packet->length = atoi(token); break;
		case 6:
			packet->weight = atof(token);
			packet->has_weight = 1;
			break;
		}
		field++;
		token = strtok(NULL, " ");
	}

	free(line_copy);



}

void add_packet_to_queue(PacketQueue* queue, const Packet* packet) {
	if (queue->count >= queue->capacity) {
		queue->capacity *= 2;
		queue->packets = realloc(queue->packets, queue->capacity * sizeof(Packet));
	}
	queue->packets[queue->count++] = *packet;
}

void remove_packet_from_queue(PacketQueue* queue, int index) {

	for (int i = index; i < queue->count - 1; i++) {
		queue->packets[i] = queue->packets[i + 1];
	}
	queue->count--;
}

void schedule_next_packet() {
	if (ready_queue.count == 0) return;

	const double EPS = 1e-9;   // tolerance for almost-equal VFTs

	int best_idx = 0;
	for (int i = 1; i < ready_queue.count; i++) {
		if (ready_queue.packets[i].is_on_bus == 0) {
			double diff = ready_queue.packets[i].virtual_finish_time -
				ready_queue.packets[best_idx].virtual_finish_time;

			if (diff < -EPS ||                            /* clearly smaller VFT          */
				(fabs(diff) <= EPS &&                   /* virtually equal → tie-break  */
					connections[ready_queue.packets[i].connection_id].appearance_order <
					connections[ready_queue.packets[best_idx].connection_id].appearance_order)) {
				best_idx = i;
			}
		}
	}
	ready_queue.packets[best_idx].is_on_bus = 1;
	Packet packet_to_send = ready_queue.packets[best_idx];
	//remove_packet_from_queue(&ready_queue, best_idx);
	is_packet_on_bus = 1;
	//packet_on_bus_idx = best_idx;
	remove_packet_from_queue(&ready_queue, best_idx);



	// Determine actual start time for this packet
	// real_time currently holds when the server *became free* from the *previous* transmission (or 0 if idle)
	long long actual_start_time = (next_departure_time > packet_to_send.arrival_time) ? next_departure_time : packet_to_send.arrival_time;

	// Original output format restored
	printf("%lld: %s\n", actual_start_time, packet_to_send.original_line);



	// Update virtual time
	// Calculate weight sum of connections that were in ready_queue *before* this packet was removed
	double current_weight_sum = 0;

	// Update server's next free time
	next_departure_time = actual_start_time + packet_to_send.length;
}


void cleanup() {
	if (pending_packets.packets) free(pending_packets.packets);
	if (ready_queue.packets) free(ready_queue.packets);
	if (virtual_bus.packets) free(virtual_bus.packets);
}

int main() {
	char line[LINE_BUFFER_SZ];
	int appearance_order = 0;
	init_packet_queue(&pending_packets);
	init_packet_queue(&ready_queue);
	init_packet_queue(&virtual_bus);
	// init_packet_queue(&to_leave_virtual_bus);
	while (fgets(line, sizeof(line), stdin)) {
		line[strcspn(line, "\n")] = 0;
		if (strlen(line) == 0) continue;
		Packet packet;
		parse_packet(line, &packet, appearance_order++);
		add_packet_to_queue(&pending_packets, &packet);
	}

	qsort(pending_packets.packets, pending_packets.count, sizeof(Packet), compare_packets_by_arrival_time);

	while (pending_packets.count > 0 || ready_queue.count > 0) {
		long long next_arrival_event_time = (pending_packets.count > 0) ? pending_packets.packets[0].arrival_time : LLONG_MAX;

		if (ready_queue.count == 0 && next_arrival_event_time == LLONG_MAX) {
			break;
		}
		current_time = next_arrival_event_time;
		if (next_departure_time < current_time && is_packet_on_bus) { //&& is_packet_on_bus) {
			current_time = next_departure_time;
		}
		if (virtual_bus.count != 0) {
			double virtual_finish = virtual_bus.packets[0].virtual_finish_time;
			double real_finish_virtual = last_virtual_change + (virtual_finish - virtual_time) * sum_Active_weights();
			if (real_finish_virtual < current_time) {
				current_time = real_finish_virtual;
				should_remove_from_virtual_bus = 1; // will remove later, need to take into account when computing virtual
			}
		}




		if (current_time == DBL_MAX) break;

		double weight_sum = sum_Active_weights();
		if (weight_sum > 0) {
			virtual_time += (double)(current_time - last_virtual_change) / weight_sum;
		}
		last_virtual_change = current_time;
		// }
		if (should_remove_from_virtual_bus) {
			should_remove_from_virtual_bus = 0;
			remove_packet_from_queue(&virtual_bus, 0);
		}

		if (current_time >= next_departure_time && is_packet_on_bus == 1) {
			is_packet_on_bus = 0;
		}
		// Process all packets that have arrived by this current_time
		if (pending_packets.count > 0 && pending_packets.packets[0].arrival_time <= current_time) {
			Packet packet = pending_packets.packets[0];
			// Find or create connection
			packet.connection_id = find_or_create_connection(packet.src_ip, packet.src_port,
				packet.dst_ip, packet.dst_port,
				appearance_order);
			remove_packet_from_queue(&pending_packets, 0);

			int conn_id = packet.connection_id;
			double last_conn_vft = connections[conn_id].virtual_finish_time;

			double virtual_start = (virtual_time > last_conn_vft) ? virtual_time : last_conn_vft;

			packet.virtual_start_time = virtual_start;
			if (packet.has_weight) {
				connections[conn_id].weight = packet.weight;
			}
			else { packet.weight = connections[conn_id].weight; } //if packet does not have a specified weight, take the connection's at the time
			packet.virtual_finish_time = virtual_start + (double)packet.length / connections[conn_id].weight;
		
			connections[conn_id].virtual_finish_time = packet.virtual_finish_time;

			add_packet_to_queue(&ready_queue, &packet);
			add_packet_to_queue(&virtual_bus, &packet);
			qsort(virtual_bus.packets, virtual_bus.count, sizeof(Packet), compare_packets_by_virtual_finish_time);
		}


		if (ready_queue.count > 0 && next_departure_time <= current_time && is_packet_on_bus == 0) {// && is_packet_on_bus == 0) { //was && real_time <= current_time
			schedule_next_packet(current_time);
		}



	}

	cleanup();
	return 0;
}
