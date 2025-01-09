#include <mpi.h>
#include <iostream>
#include <vector>
using namespace std;

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int V, E, K, R, L;
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
        cin >> R;
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

    // Determine local vertices for each rank
    int vertices_per_process = V / size;
    int start_vertex = rank * vertices_per_process;
    int end_vertex = (rank == size - 1) ? V : start_vertex + vertices_per_process;

    // Extract local adjacency list
    vector<vector<int>> local_adj_list(end_vertex - start_vertex);
    for (int i = start_vertex; i < end_vertex; i++) {
        local_adj_list[i - start_vertex] = adj_list[i];
    }

    // Debug: Print local adjacency list
    for (int i = start_vertex; i < end_vertex; i++) {
        cout << "Rank " << rank << " Vertex " << i << " -> ";
        for (int neighbor : local_adj_list[i - start_vertex]) {
            cout << neighbor << " ";
        }
        cout << endl;
    }

    

    MPI_Finalize();
    return 0;
}
