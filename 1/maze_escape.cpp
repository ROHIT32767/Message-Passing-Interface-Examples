#include <bits/stdc++.h>
#include <mpi.h>
#include <iostream>
#include <vector>
#include <queue>
#include <set>
using namespace std;

int get_process_number(int vertex, int total_processes, int total_vertices) {
    int div = total_vertices / total_processes;
    int rem = total_vertices % total_processes;
    if (vertex < rem * (div + 1)) {
        return vertex / (div + 1);
    } else {
        return rem + (vertex - rem * (div + 1)) / div;
    }
}

pair<int,int> get_vertex_range(int process_number, int total_processes, int total_vertices) {
    int div = total_vertices / total_processes;
    int rem = total_vertices % total_processes;
    int start_vertex, end_vertex;
    if(process_number < rem) {
        start_vertex = process_number * (div + 1);
        end_vertex = start_vertex + div; 
    } else {
        start_vertex = rem * (div + 1) + (process_number - rem) * div;
        end_vertex = start_vertex + div - 1;
    }
    if(start_vertex > end_vertex){
        return {INT_MAX, INT_MIN};
    }
    return {start_vertex, end_vertex};
}   

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
        cin >> R;  // Source vertex
        cin >> K;
        starting_nodes.resize(K);
        for (int i = 0; i < K; i++) {
            cin >> starting_nodes[i];
        }
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

    pair<int, int> vertex_range = get_vertex_range(rank, size, V);
    int start_vertex = vertex_range.first;
    int end_vertex = vertex_range.second;

    // Initialize BFS variables
    vector<int> Lvs(V, -1);  // Initialize levels of vertices
    set<int> local_frontier;
    
    // Source vertex R initialization
    if (R >= start_vertex && R <= end_vertex) {
        Lvs[R] = 0;
        local_frontier.insert(R);
    }

    bool global_active = true;
    int level = 0;
    while (global_active) {
        set<int> new_neighbors;
        set<int> frontier_copy = local_frontier;

        // Process local frontier
        for (int v : frontier_copy) {
            local_frontier.erase(v);  // Remove from local frontier as it is being processed

            for (int neighbor : adj_list[v]) {
                if (Lvs[neighbor] == -1) {  // If the vertex is not visited
                    if (neighbor >= start_vertex && neighbor <= end_vertex) {
                        // Local node: Mark its level to level + 1
                        Lvs[neighbor] = level + 1;
                        local_frontier.insert(neighbor);
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

        for (int p = 0; p < size; p++){
            if (p == rank) continue;
            // Send data to other processes
            int send_size = to_send.size();
            MPI_Send(&send_size, 1, MPI_INT, p, 0, MPI_COMM_WORLD);
            MPI_Send(to_send.data(), send_size, MPI_INT, p, 0, MPI_COMM_WORLD);

            // Receive data from other processes
            int recv_size;
            MPI_Recv(&recv_size, 1, MPI_INT, p, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            vector<int> temp(recv_size);
            MPI_Recv(temp.data(), recv_size, MPI_INT, p, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            recv_buffer.insert(recv_buffer.end(), temp.begin(), temp.end());
        }

        // Add received nodes to local frontier if not visited
        for (int neighbor : recv_buffer) {
            if (Lvs[neighbor] == -1) {
                // If not visited, mark it with level + 1
                Lvs[neighbor] = level + 1;
                if (neighbor >= start_vertex && neighbor <= end_vertex) {
                    local_frontier.insert(neighbor);
                }
            }
        }

        // Check if global frontier is active
        global_active = !local_frontier.empty();
        MPI_Allreduce(MPI_IN_PLACE, &global_active, 1, MPI_C_BOOL, MPI_LOR, MPI_COMM_WORLD);

        if (global_active) {
            level++;  // Increase level after processing current level
        }
    }

    cout << "start_vertex: " << start_vertex << " end_vertex: " << end_vertex << " rank: " << rank << endl;

    // Output BFS levels for debugging
    for (int i = start_vertex; i <= end_vertex; i++) {
        if (Lvs[i] != -1) {
            cout << "Rank " << rank << " Vertex " << i << " Level: " << Lvs[i] << endl;
        }
    }

    MPI_Finalize();
    return 0;
}
