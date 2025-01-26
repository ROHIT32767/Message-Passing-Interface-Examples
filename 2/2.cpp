#include <stddef.h>
#include <iostream>
#include <algorithm>
#include "mpi.h"
#include <set>
#include <map>
#include <bits/stdc++.h>

using namespace std;
struct stone
{
    int stone_index;
    int x;
    int y;
    char direction;
    bool operator==(const stone &stone_particle) const
    {
        return (x == stone_particle.x && y == stone_particle.y and direction == stone_particle.direction);
    }
};

bool compareParticles(const stone &stone_particle_1, const stone &stone_particle_2)
{
    return stone_particle_2.stone_index > stone_particle_1.stone_index;
}

bool shouldRemoveParticle(const stone &stone_particle)
{
    if(stone_particle.x == -1 && stone_particle.y == -1){
        return true;
    }
    return false;
}

pair<int, int> get_row_range(int process_number, int total_processes, int total_rows)
{
    int div = total_rows / total_processes;
    int rem = total_rows % total_processes;
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

int get_process_number(int row, int total_processes, int total_rows)
{
    int div = total_rows / total_processes;
    int rem = total_rows % total_processes;
    if (row < rem * (div + 1))
    {
        return row / (div + 1);
    }
    else
    {
        return rem + (row - rem * (div + 1)) / div;
    }
}

MPI_Datatype MPI_PARTICLE;

char nextDirection(char direction)
{
    if (direction == 'D')
        return 'L';
    else if (direction == 'L')
        return 'U';
    else if (direction == 'U')
        return 'R';
    else if (direction == 'R')
        return 'D';
    return direction;
}

char opposite_direction(char direction)
{
    if (direction == 'D')
        return 'U';
    else if (direction == 'L')
        return 'R';
    else if (direction == 'U')
        return 'D';
    else if (direction == 'R')
        return 'L';
    return direction;
}

void handle_collision(vector<stone> &stones)
{
    map<int, int> collision_index_map;
    for (int i = 0; i <= stones.size() - 1; i++)
    {
        collision_index_map[stones[i].stone_index] = i;
    }
    map< pair<int, int>, set<int> > coordinate_index_map;
    for (int i = 0; i <= stones.size() - 1; i++)
    {
        coordinate_index_map[make_pair(stones[i].x, stones[i].y)].insert(stones[i].stone_index);
    }
    for (auto &entry : coordinate_index_map)
    {
        auto &position = entry.first;
        auto &particle_indices = entry.second;
        int count = particle_indices.size();
        if (count == 2)
        {
            auto it = particle_indices.begin();
            int stone_one_collision = *it;
            it++;
            int stone_two_collision = *it;
            int ind1 = -1, ind2 = -1;
            ind1 = collision_index_map[stone_one_collision];
            ind2 = collision_index_map[stone_two_collision];
            if (ind1 != -1 && ind2 != -1)
            {
                stones[ind1].direction = nextDirection(stones[ind1].direction);
                stones[ind2].direction = nextDirection(stones[ind2].direction);
            }
        }
        else if (count == 4)
        {
            auto it = particle_indices.begin();
            int stone_one_collision = *it;
            it++;
            int stone_two_collision = *it;
            it++;
            int stone_three_collision = *it;
            it++;
            int stone_four_collision = *it;
            int ind1 = -1, ind2 = -1, ind3 = -1, ind4 = -1;
            ind1 = collision_index_map[stone_one_collision];
            ind2 = collision_index_map[stone_two_collision];
            ind3 = collision_index_map[stone_three_collision];
            ind4 = collision_index_map[stone_four_collision];
            if (ind1 != -1 && ind2 != -1 && ind3 != -1 && ind4 != -1)
            {
                stones[ind1].direction = opposite_direction(stones[ind1].direction);
                stones[ind2].direction = opposite_direction(stones[ind2].direction);
                stones[ind3].direction = opposite_direction(stones[ind3].direction);
                stones[ind4].direction = opposite_direction(stones[ind4].direction);
            }
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
}

int main(int argc, char *argv[])
{
    ios_base::sync_with_stdio(false);
    cin.tie(NULL);
    MPI_Init(&argc, &argv);
    int rank, size;
    string input_file = argv[1];
    string output_file = argv[2];
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int N, M, K, T;
    if (rank == 0)
    {
        freopen(input_file.c_str(), "r", stdin);
        cin >> N >> M >> K >> T;
    }
    MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&M, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&K, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&T, 1, MPI_INT, 0, MPI_COMM_WORLD);
    int blocklength = sizeof(stone);
    MPI_Type_contiguous(blocklength, MPI_BYTE, &MPI_PARTICLE);
    MPI_Type_commit(&MPI_PARTICLE);
    vector<stone> stones(K);
    vector<int> particleCount(size, 0);
    if (rank == 0)
    {
        for (int i = 0; i <= K-1; i++)
        {
            cin >> stones[i].x >> stones[i].y >> stones[i].direction;
            stones[i].stone_index = i;
        }
        for (auto &temp_stone : stones)
        {
            int process = get_process_number(temp_stone.y, size, M);
            MPI_Send(&temp_stone, 1, MPI_PARTICLE, process, 0, MPI_COMM_WORLD);
            particleCount[process]++;
        }
        fclose(stdin);
    }
    MPI_Bcast(&particleCount[0], size, MPI_INT, 0, MPI_COMM_WORLD);
    int num_particles = particleCount[rank];
    stones.resize(num_particles);
    for (int i = 0; i <= num_particles - 1; i++)
    {
        MPI_Recv(&stones[i], 1, MPI_PARTICLE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    pair<int, int> row_range = get_row_range(rank, size, M);
    int start_row = row_range.first;
    int end_row = row_range.second;
    int root = 0;
    MPI_Barrier(MPI_COMM_WORLD);
    for (int i = 0; i <= T-1; i++)
    {
        for (auto &temp_stone : stones)
        {
            if (temp_stone.x == 0 && temp_stone.direction == 'U')
            {
                temp_stone.x = N;
            }
            else if (temp_stone.x == N - 1 && temp_stone.direction == 'D')
            {
                temp_stone.x = -1;
            }
            else if (temp_stone.y == 0 && temp_stone.direction == 'L')
            {
                temp_stone.y = M;
            }
            else if (temp_stone.y == M - 1 && temp_stone.direction == 'R')
            {
                temp_stone.y = -1;
            }
        }
        MPI_Barrier(MPI_COMM_WORLD);
        for (auto &temp_stone : stones)
        {
            MPI_Request request;
            if (temp_stone.y == -1 && temp_stone.direction == 'R')
            {
                int proc_num = get_process_number(0, size, M);
                MPI_Send(&temp_stone, 1, MPI_PARTICLE, proc_num, 0, MPI_COMM_WORLD);
                temp_stone.x = -1;
                temp_stone.y = -1;
            }
            else if (temp_stone.y == M && temp_stone.direction == 'L')
            {
                int proc_num = get_process_number(M - 1, size, M);
                MPI_Send(&temp_stone, 1, MPI_PARTICLE, proc_num, 0, MPI_COMM_WORLD);
                temp_stone.x = -1;
                temp_stone.y = -1;
            }
            else if (temp_stone.y == start_row && (rank - 1) >= 0 && temp_stone.direction == 'L')
            {
                MPI_Send(&temp_stone, 1, MPI_PARTICLE, rank - 1, 0, MPI_COMM_WORLD);
                temp_stone.x = -1;
                temp_stone.y = -1;
            }
            else if (temp_stone.y == end_row && (rank + 1) < size && temp_stone.direction == 'R')
            {
                MPI_Send(&temp_stone, 1, MPI_PARTICLE, rank + 1, 0, MPI_COMM_WORLD);
                temp_stone.x = -1;
                temp_stone.y = -1;
            }
        }
        stones.erase(std::remove_if(stones.begin(), stones.end(), shouldRemoveParticle), stones.end());
        MPI_Barrier(MPI_COMM_WORLD);
        if (rank + 2 <= size)
        {
            MPI_Status status;
            MPI_Request request;
            int flag = 0;
            MPI_Iprobe(rank + 1, 0, MPI_COMM_WORLD, &flag, &status);
            while (flag)
            {
                stone p;
                MPI_Recv(&p, 1, MPI_PARTICLE, rank + 1, 0, MPI_COMM_WORLD, &status);
                stones.push_back(p);
                MPI_Iprobe(rank + 1, 0, MPI_COMM_WORLD, &flag, &status);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);

        if (rank >= 1)
        {
            MPI_Status status;
            MPI_Request request;
            int flag = 0;
            MPI_Iprobe(rank - 1, 0, MPI_COMM_WORLD, &flag, &status);
            while (flag)
            {
                stone p;
                MPI_Recv(&p, 1, MPI_PARTICLE, rank - 1, 0, MPI_COMM_WORLD, &status);
                stones.push_back(p);
                MPI_Iprobe(rank - 1, 0, MPI_COMM_WORLD, &flag, &status);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);

        if (rank == 0)
        {
            MPI_Status status;
            MPI_Request request;
            int flag = 0;
            int last_process_allocated = get_process_number(M - 1, size, M);
            MPI_Iprobe(last_process_allocated, 0, MPI_COMM_WORLD, &flag, &status);
            while (flag)
            {
                stone p;
                MPI_Recv(&p, 1, MPI_PARTICLE, last_process_allocated, 0, MPI_COMM_WORLD, &status);
                stones.push_back(p);
                MPI_Iprobe(last_process_allocated, 0, MPI_COMM_WORLD, &flag, &status);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);

        int last_process_allocated_rank = get_process_number(M - 1, size, M);
        if (rank == last_process_allocated_rank)
        {
            MPI_Status status;
            MPI_Request request;
            int flag = 0;
            int first_process_allocated = get_process_number(0, size, M);
            MPI_Iprobe(first_process_allocated, 0, MPI_COMM_WORLD, &flag, &status);
            while (flag)
            {
                stone p;
                MPI_Recv(&p, 1, MPI_PARTICLE, first_process_allocated, 0, MPI_COMM_WORLD, &status);
                stones.push_back(p);
                MPI_Iprobe(first_process_allocated, 0, MPI_COMM_WORLD, &flag, &status);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);
        for (auto &temp_stone : stones)
        {
            int x = temp_stone.x;
            int y = temp_stone.y;
            if (temp_stone.direction == 'L')
            {
                y = y-1;
            }
            else if (temp_stone.direction == 'R')
            {
                y = y+1;
            }
            else if (temp_stone.direction == 'U')
            {
                x = x-1;
            }
            else if (temp_stone.direction == 'D')
            {
                x = x+1;
            }
            temp_stone.x = x;
            temp_stone.y = y;
        }
        MPI_Barrier(MPI_COMM_WORLD);
        handle_collision(stones);
        MPI_Barrier(MPI_COMM_WORLD);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    vector<stone> collected_stones;
    int total_particles = 0;
    int particle_size = stones.size();
    vector<int> count_received(size);
    vector<int> displs(size);
    MPI_Gather(&particle_size, 1, MPI_INT, &count_received[0], 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (rank == 0)
    {
        total_particles = accumulate(count_received.begin(), count_received.end(), 0);
        collected_stones.resize(total_particles);
        displs[0] = 0;
        for (int i = 0; i < size-1; i++)
        {
            displs[i+1] = displs[i];
            displs[i+1] += count_received[i];
        }
    }
    MPI_Gatherv(&stones[0], stones.size(), MPI_PARTICLE, &collected_stones[0], &count_received[0], &displs[0], MPI_PARTICLE, 0, MPI_COMM_WORLD);
    if (rank == 0)
    {
        freopen(output_file.c_str(), "w", stdout);
        sort(collected_stones.begin(), collected_stones.end(), compareParticles);
        for (auto &temp_stone : collected_stones)
        {
            cout << temp_stone.x << " " << temp_stone.y << " " << temp_stone.direction << endl;
        }
        fclose(stdout);
    }
    MPI_Finalize();
    return 0;
}