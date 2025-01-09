#include <bits/stdc++.h>
#include <mpi.h>
#include <iostream>
#include <vector>
#include <queue>
#include <set>
using namespace std;

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int V, E, R, K, L;
    vector<vector<int>> adj_list;
    vector<int> blocked_nodes;
    vector<int> starting_nodes;

    if (rank == 0) {
        // Input from Rank 0
        cin >> V >> E;
        adj_list.resize(V);
        for (int i = 0; i < E; i++) {
            int u, v, d;
            cin >> u >> v >> d;
            if (d == 1) {
                adj_list[u].push_back(v);
                adj_list[v].push_back(u);
            } else {
                adj_list[u].push_back(v);
            }
        }
        cin >> K;
        starting_nodes.resize(K);
        for (int i = 0; i < K; i++) {
            cin >> starting_nodes[i];
        }
        cin >> R;  // Source vertex
        cin >> L;
        blocked_nodes.resize(L);
        for (int i = 0; i < L; i++) {
            cin >> blocked_nodes[i];
        }
    }

    // Broadcast global data
    MPI_Bcast(&V, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&E, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&R, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&K, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&L, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank != 0) {
        adj_list.resize(V);
    }

    // Broadcast adjacency list sizes
    vector<int> adj_sizes(V);
    if (rank == 0) {
        for (int i = 0; i < V; i++) {
            adj_sizes[i] = adj_list[i].size();
        }
    }
    MPI_Bcast(adj_sizes.data(), V, MPI_INT, 0, MPI_COMM_WORLD);

    // Allocate space for adjacency lists
    for (int i = 0; i < V; i++) {
        adj_list[i].resize(adj_sizes[i]);
    }

    // Broadcast adjacency list data
    for (int i = 0; i < V; i++) {
        MPI_Bcast(adj_list[i].data(), adj_sizes[i], MPI_INT, 0, MPI_COMM_WORLD);
    }

    // Broadcast starting nodes and blocked nodes
    vector<int> global_starting_nodes(K);
    vector<int> global_blocked_nodes(L);

    if (rank == 0) {
        global_starting_nodes = starting_nodes;
        global_blocked_nodes = blocked_nodes;
    }

    MPI_Bcast(global_starting_nodes.data(), K, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(global_blocked_nodes.data(), L, MPI_INT, 0, MPI_COMM_WORLD);

    // Determine local vertices for each rank
    int vertices_per_process = V / size;
    int start_vertex = rank * vertices_per_process;
    int end_vertex = (rank == size - 1) ? V : start_vertex + vertices_per_process;

    // Check if the current process owns the source vertex R
    bool owns_R = (R >= start_vertex && R < end_vertex);

    // Initialize BFS variables
    vector<bool> visited(V, false);
    vector<int> levels(V, -1);
    queue<int> local_frontier;

    // Mark blocked nodes as visited
    set<int> blocked_set(global_blocked_nodes.begin(), global_blocked_nodes.end());
    for (int blocked : blocked_set) {
        visited[blocked] = true;
    }

    // Initialize the BFS for explorers' starting nodes
    vector<int> active_explorers;
    for (int i = 0; i < K; i++) {
        if (global_starting_nodes[i] >= start_vertex && global_starting_nodes[i] < end_vertex) {
            int start_node = global_starting_nodes[i];
            visited[start_node] = true;
            levels[start_node] = 0;
            local_frontier.push(start_node);
        } else {
            active_explorers.push_back(global_starting_nodes[i]);
        }
    }

    bool global_active = true;
    while (global_active) {
        set<int> new_neighbors;

        // Process local frontier
        int frontier_size = local_frontier.size();
        for (int i = 0; i < frontier_size; i++) {
            int current = local_frontier.front();
            local_frontier.pop();

            for (int neighbor : adj_list[current]) {
                if (!visited[neighbor] && blocked_set.find(neighbor) == blocked_set.end()) {
                    if (neighbor >= start_vertex && neighbor < end_vertex) {
                        // Local node
                        local_frontier.push(neighbor);
                        visited[neighbor] = true;
                        levels[neighbor] = levels[current] + 1;
                    } else {
                        // Non-local node
                        new_neighbors.insert(neighbor);
                    }
                }
            }
        }

        // Share new neighbors with other processes
        vector<int> to_send(new_neighbors.begin(), new_neighbors.end());
        vector<int> recv_buffer;

        for (int p = 0; p < size; p++) {
            if (p == rank) continue;
            int send_size = to_send.size();
            MPI_Send(&send_size, 1, MPI_INT, p, 0, MPI_COMM_WORLD);
            MPI_Send(to_send.data(), send_size, MPI_INT, p, 0, MPI_COMM_WORLD);

            int recv_size;
            MPI_Recv(&recv_size, 1, MPI_INT, p, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            vector<int> temp(recv_size);
            MPI_Recv(temp.data(), recv_size, MPI_INT, p, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            recv_buffer.insert(recv_buffer.end(), temp.begin(), temp.end());
        }

        // Add received nodes to local frontier if not visited
        for (int neighbor : recv_buffer) {
            if (!visited[neighbor] && blocked_set.find(neighbor) == blocked_set.end() && neighbor >= start_vertex && neighbor < end_vertex) {
                local_frontier.push(neighbor);
                visited[neighbor] = true;
                levels[neighbor] = levels[R] + 1;  // Update level relative to R
            }
        }

        // Check if global frontier is active
        global_active = !local_frontier.empty();
        MPI_Allreduce(MPI_IN_PLACE, &global_active, 1, MPI_C_BOOL, MPI_LOR, MPI_COMM_WORLD);
    }

    // Output BFS levels for the visited nodes
    for (int i = start_vertex; i < end_vertex; i++) {
        if (levels[i] != -1) {
            cout << "Rank " << rank << " Vertex " << i << " Level: " << levels[i] << endl;
        }
    }

    MPI_Finalize();
    return 0;
}
