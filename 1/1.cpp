#include <bits/stdc++.h>
#include <mpi.h>
#include <iostream>
#include <vector>
#include <queue>
#include <set>
using namespace std;

int get_process_number(int vertex, int total_processes, int total_vertices)
{
    int div = total_vertices / total_processes;
    int rem = total_vertices % total_processes;
    if (vertex < rem * (div + 1))
    {
        return vertex / (div + 1);
    }
    else
    {
        return rem + (vertex - rem * (div + 1)) / div;
    }
}

pair<int, int> get_vertex_range(int process_number, int total_processes, int total_vertices)
{
    int div = total_vertices / total_processes;
    int rem = total_vertices % total_processes;
    int start_vertex, end_vertex;
    if (process_number < rem)
    {
        start_vertex = process_number * (div + 1);
        end_vertex = start_vertex + div;
    }
    else
    {
        start_vertex = rem * (div + 1) + (process_number - rem) * div;
        end_vertex = start_vertex + div - 1;
    }
    if (start_vertex > end_vertex)
    {
        return make_pair(1000, -1000);
    }
    return make_pair(start_vertex, end_vertex);
}

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);
    string input_file = argv[1];
    string output_file = argv[2];
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int V, E, R, K, L;
    vector<vector<int>> adj_list;
    vector<int> blocked_nodes;
    vector<int> starting_nodes;
    vector<vector<int>> local_adj_list;
    vector<int> local_level_array;
    set<int> blocked_set;

    if (rank == 0)
    {
        freopen(input_file.c_str(), "r", stdin);
        cin >> V >> E;
        adj_list.resize(V);
        for (int i = 0; i < E; i++)
        {
            int u, v, d;
            cin >> u >> v >> d;
            if (d == 1)
            {
                adj_list[u].push_back(v);
                adj_list[v].push_back(u);
            }
            else
            {
                adj_list[v].push_back(u);
            }
        }
        cin >> K;
        starting_nodes.resize(K);
        for (int i = 0; i < K; i++)
        {
            cin >> starting_nodes[i];
        }
        cin >> R;
        cin >> L;
        if (L > 0)
        {
            blocked_nodes.resize(L);
        }
        for (int i = 0; i < L; i++)
        {
            cin >> blocked_nodes[i];
        }
        if (L > 0)
        {
            blocked_set.insert(blocked_nodes.begin(), blocked_nodes.end());
        }
        fclose(stdin);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Bcast(&V, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&E, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&R, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&K, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&L, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank != 0)
    {
        blocked_nodes.resize(L);
    }

    MPI_Bcast(blocked_nodes.data(), L, MPI_INT, 0, MPI_COMM_WORLD);
    blocked_set.insert(blocked_nodes.begin(), blocked_nodes.end());

    if (rank != 0)
    {
        pair<int, int> vertex_range = get_vertex_range(rank, size, V);
        int start = vertex_range.first;
        int end = vertex_range.second;
        if (start <= end)
        {

            int num_vertices;
            MPI_Recv(&num_vertices, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            vector<int> sizes(num_vertices);
            MPI_Recv(sizes.data(), num_vertices, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            int total_data = accumulate(sizes.begin(), sizes.end(), 0);
            vector<int> flat_data(total_data);
            MPI_Recv(flat_data.data(), total_data, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            local_adj_list.resize(num_vertices);
            int index = 0;
            for (int i = 0; i < num_vertices; i++)
            {
                local_adj_list[i].assign(flat_data.begin() + index, flat_data.begin() + index + sizes[i]);
                index += sizes[i];
            }
        }
    }
    else
    {
        for (int p = 1; p < size; p++)
        {
            pair<int, int> range = get_vertex_range(p, size, V);
            int start_vertex = range.first;
            int end_vertex = range.second;

            if (start_vertex > end_vertex)
            {
                continue;
            }

            vector<int> sizes, flat_data;
            for (int i = start_vertex; i <= end_vertex; i++)
            {
                sizes.push_back(adj_list[i].size());
                flat_data.insert(flat_data.end(), adj_list[i].begin(), adj_list[i].end());
            }

            int num_vertices = sizes.size();
            MPI_Send(&num_vertices, 1, MPI_INT, p, 0, MPI_COMM_WORLD);
            MPI_Send(sizes.data(), num_vertices, MPI_INT, p, 0, MPI_COMM_WORLD);
            MPI_Send(flat_data.data(), flat_data.size(), MPI_INT, p, 0, MPI_COMM_WORLD);
        }

        pair<int, int> rank0_range = get_vertex_range(0, size, V);
        int start_vertex = rank0_range.first;
        int end_vertex = rank0_range.second;
        local_adj_list.resize(end_vertex - start_vertex + 1);
        for (int i = start_vertex; i <= end_vertex; i++)
        {
            local_adj_list[i - start_vertex] = adj_list[i];
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0)
    {
        adj_list.clear();
    }

    pair<int, int> vertex_range = get_vertex_range(rank, size, V);
    int start_vertex = vertex_range.first;
    int end_vertex = vertex_range.second;

    if (start_vertex <= end_vertex)
    {
        local_level_array.resize(end_vertex - start_vertex + 1, -1);
    }

    set<int> local_frontier;

    if (R >= start_vertex && R <= end_vertex)
    {
        if (blocked_set.find(R) == blocked_set.end())
        {
            local_level_array[R - start_vertex] = 0;
            local_frontier.insert(R);
        }
    }

    bool global_active = true;
    int level = 0;
    while (global_active)
    {
        set<int> new_neighbors;
        set<int> frontier_copy = local_frontier;

        for (int v : frontier_copy)
        {
            local_frontier.erase(v);

            for (int neighbor : local_adj_list[v - start_vertex])
            {
                if (blocked_set.find(neighbor) != blocked_set.end())
                {
                    continue;
                }
                else if (neighbor >= start_vertex && neighbor <= end_vertex)
                {
                    if (local_level_array[neighbor - start_vertex] == -1)
                    {
                        local_level_array[neighbor - start_vertex] = level + 1;
                        local_frontier.insert(neighbor);
                    }
                }
                else
                {
                    new_neighbors.insert(neighbor);
                }
            }
        }

        vector<int> to_send(new_neighbors.begin(), new_neighbors.end());
        vector<int> recv_buffer;

        vector<vector<int>> to_send_to(size);
        for (int neighbor : to_send)
        {
            int owner_process = get_process_number(neighbor, size, V);
            if (owner_process != rank)
            {
                to_send_to[owner_process].push_back(neighbor);
            }
        }

        for (int p = 0; p < size; p++)
        {
            if (p == rank)
                continue;

            int send_size = to_send_to[p].size();
            MPI_Send(&send_size, 1, MPI_INT, p, 0, MPI_COMM_WORLD);
            if (send_size > 0)
            {
                MPI_Send(to_send_to[p].data(), send_size, MPI_INT, p, 0, MPI_COMM_WORLD);
            }

            int recv_size;
            MPI_Recv(&recv_size, 1, MPI_INT, p, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (recv_size > 0)
            {
                vector<int> temp(recv_size);
                MPI_Recv(temp.data(), recv_size, MPI_INT, p, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                recv_buffer.insert(recv_buffer.end(), temp.begin(), temp.end());
            }
        }

        for (int neighbor : recv_buffer)
        {
            if (blocked_set.find(neighbor) != blocked_set.end())
            {
                continue;
            }
            else if (neighbor >= start_vertex && neighbor <= end_vertex)
            {
                if (local_level_array[neighbor - start_vertex] == -1)
                {
                    local_level_array[neighbor - start_vertex] = level + 1;
                    local_frontier.insert(neighbor);
                }
            }
        }

        global_active = !local_frontier.empty();
        MPI_Allreduce(MPI_IN_PLACE, &global_active, 1, MPI_C_BOOL, MPI_LOR, MPI_COMM_WORLD);

        if (global_active)
        {
            level++;
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    vector<int> full_level_array(V, -1);
    vector<int> send_counts(size, 0);
    vector<int> displs(size, 0);

    if (rank == 0)
    {
        for (int i = 0; i < size; i++)
        {
            pair<int, int> range = get_vertex_range(i, size, V);
            send_counts[i] = max(0, range.second - range.first + 1);
        }
        partial_sum(send_counts.begin(), send_counts.end() - 1, displs.begin() + 1);
    }

    MPI_Gatherv(local_level_array.data(), local_level_array.size(), MPI_INT,
                full_level_array.data(), send_counts.data(), displs.data(), MPI_INT, 0, MPI_COMM_WORLD);

    if (rank == 0)
    {
        freopen(output_file.c_str(), "w", stdout);
        for (int i = 0; i < K; i++)
        {
            cout << full_level_array[starting_nodes[i]] << " ";
        }
        fclose(stdout);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    return 0;
}
