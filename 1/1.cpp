#include <mpi.h>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <limits>
#include <set>

using namespace std;

const int INF = numeric_limits<int>::max();

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int V, E, k, R, L;
    vector<pair<int, pair<int, int>>> edges; // {u, {v, d}}
    vector<int> starting_nodes;
    vector<int> blocked_nodes;

    if (rank == 0) {
        // Input reading (only by master process)
        cin >> V >> E;
        edges.resize(E);
        for (int i = 0; i < E; ++i) {
            int u, v, d;
            cin >> u >> v >> d;
            edges[i] = {u, {v, d}};
        }

        cin >> k;
        starting_nodes.resize(k);
        for (int i = 0; i < k; ++i) cin >> starting_nodes[i];

        cin >> R;
        cin >> L;
        blocked_nodes.resize(L);
        for (int i = 0; i < L; ++i) cin >> blocked_nodes[i];
    }

    // Broadcast data to all processes
    MPI_Bcast(&V, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&E, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&R, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank != 0) edges.resize(E);
    MPI_Bcast(edges.data(), E * sizeof(pair<int, pair<int, int>>), MPI_BYTE, 0, MPI_COMM_WORLD);

    if (rank == 0) MPI_Bcast(starting_nodes.data(), k, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank == 0) MPI_Bcast(blocked_nodes.data(), L, MPI_INT, 0, MPI_COMM_WORLD);

    // Build adjacency list
    unordered_map<int, vector<int>> adj;
    for (const auto& edge : edges) {
        int u = edge.first, v = edge.second.first, d = edge.second.second;
        adj[u].push_back(v);
        if (d == 1) adj[v].push_back(u); // Add reverse edge for bidirectional
    }

    // Mark blocked nodes
    unordered_set<int> blocked(blocked_nodes.begin(), blocked_nodes.end());

    // Initialize levels and frontier
    vector<int> levels(V, INF);
    set<int> frontier;
    if (rank == 0 && blocked.find(R) == blocked.end()) {
        levels[R] = 0;
        frontier.insert(R);
    }

    // 2D partitioning: divide processes into rows and columns
    int sqrt_p = (int)sqrt(size);
    if (sqrt_p * sqrt_p != size) {
        if (rank == 0) cerr << "Number of processes must be a perfect square." << endl;
        MPI_Finalize();
        return 1;
    }

    int row = rank / sqrt_p;
    int col = rank % sqrt_p;

    set<int> local_frontier;

    while (true) {
        // Each processor in the same column sends its frontier to others in the column
        vector<int> send_buffer(frontier.begin(), frontier.end());
        int send_count = send_buffer.size();
        vector<int> recv_counts(sqrt_p);

        MPI_Allgather(&send_count, 1, MPI_INT, recv_counts.data(), 1, MPI_INT, MPI_COMM_WORLD);

        int total_recv = 0;
        vector<int> displs(sqrt_p, 0);
        for (int i = 0; i < sqrt_p; ++i) {
            displs[i] = total_recv;
            total_recv += recv_counts[i];
        }

        vector<int> recv_buffer(total_recv);
        MPI_Allgatherv(send_buffer.data(), send_count, MPI_INT,
                       recv_buffer.data(), recv_counts.data(), displs.data(), MPI_INT,
                       MPI_COMM_WORLD);

        // Merge received frontiers
        local_frontier.clear();
        for (int node : recv_buffer) {
            local_frontier.insert(node);
        }

        // Compute neighbors of the local frontier
        set<int> next_frontier;
        for (int node : local_frontier) {
            for (int neighbor : adj[node]) {
                if (blocked.find(neighbor) == blocked.end() && levels[neighbor] == INF) {
                    levels[neighbor] = levels[node] + 1;
                    next_frontier.insert(neighbor);
                }
            }
        }

        // Check if all frontiers are empty
        int local_empty = next_frontier.empty() ? 1 : 0;
        int global_empty;
        MPI_Allreduce(&local_empty, &global_empty, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

        if (global_empty) break;

        frontier = next_frontier;
    }

    // Gather levels for starting nodes
    vector<int> results;
    if (rank == 0) results.resize(k);

    vector<int> local_results;
    for (int start : starting_nodes) {
        local_results.push_back(levels[start] == INF ? -1 : levels[start]);
    }

    MPI_Gather(local_results.data(), k, MPI_INT, results.data(), k, MPI_INT, 0, MPI_COMM_WORLD);

    // Output results from the master process
    if (rank == 0) {
        for (int dist : results) {
            cout << dist << " ";
        }
        cout << endl;
    }

    MPI_Finalize();
    return 0;
}
