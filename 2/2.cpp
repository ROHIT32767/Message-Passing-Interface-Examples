#include <stddef.h>
#include <iostream>
#include <algorithm>
#include "mpi.h"
#include <set>
#include <map>
#include <bits/stdc++.h>

using namespace std;
struct particle
{
    int ind;
    int x, y;
    char dir;
    bool operator==(const particle &p1) const
    {
        return (x == p1.x && y == p1.y and dir == p1.dir);
    }
};

bool compareParticles(const particle &p1, const particle &p2)
{
    return p1.ind < p2.ind;
}

bool shouldRemoveParticle(const particle &p)
{
    return p.x == -1 && p.y == -1;
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

char nextDirection(char dir)
{
    if (dir == 'D')
        return 'L';
    else if (dir == 'L')
        return 'U';
    else if (dir == 'U')
        return 'R';
    else if (dir == 'R')
        return 'D';
    return dir;
}

char opposite_direction(char dir)
{
    if (dir == 'D')
        return 'U';
    else if (dir == 'L')
        return 'R';
    else if (dir == 'U')
        return 'D';
    else if (dir == 'R')
        return 'L';
    return dir;
}

void collisionHandle(vector<particle> &particles)
{
    map<int, int> mp;
    for (int i = 0; i < particles.size(); i++)
    {
        mp[particles[i].ind] = i;
    }
    map< pair<int, int>, set<int> > indexParticles;
    for (int i = 0; i < particles.size(); i++)
    {
        indexParticles[make_pair(particles[i].x, particles[i].y)].insert(particles[i].ind);
    }

    for (auto &entry : indexParticles)
    {
        auto &position = entry.first;
        auto &particle_indices = entry.second;
        int count = particle_indices.size();
        if (count == 2)
        {
            auto it = particle_indices.begin();
            int ind11 = *it;
            it++;
            int ind22 = *it;
            int ind1 = -1, ind2 = -1;
            ind1 = mp[ind11];
            ind2 = mp[ind22];
            if (ind1 != -1 && ind2 != -1)
            {
                particles[ind1].dir = nextDirection(particles[ind1].dir);
                particles[ind2].dir = nextDirection(particles[ind2].dir);
            }
        }
        else if (count == 4)
        {
            auto it = particle_indices.begin();
            int ind11 = *it;
            it++;
            int ind22 = *it;
            it++;
            int ind33 = *it;
            it++;
            int ind44 = *it;
            int ind1 = -1, ind2 = -1, ind3 = -1, ind4 = -1;
            ind1 = mp[ind11];
            ind2 = mp[ind22];
            ind3 = mp[ind33];
            ind4 = mp[ind44];
            if (ind1 != -1 && ind2 != -1 && ind3 != -1 && ind4 != -1)
            {
                particles[ind1].dir = opposite_direction(particles[ind1].dir);
                particles[ind2].dir = opposite_direction(particles[ind2].dir);
                particles[ind3].dir = opposite_direction(particles[ind3].dir);
                particles[ind4].dir = opposite_direction(particles[ind4].dir);
            }
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
}

int main(int argc, char *argv[])
{
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
    int blocklength = sizeof(particle);
    MPI_Type_contiguous(blocklength, MPI_BYTE, &MPI_PARTICLE);
    MPI_Type_commit(&MPI_PARTICLE);
    vector<particle> particles(K);
    vector<int> particleCount(size, 0);
    if (rank == 0)
    {
        for (int i = 0; i < K; i++)
        {
            cin >> particles[i].x >> particles[i].y >> particles[i].dir;
            particles[i].ind = i;
        }
        for (auto &particle_var : particles)
        {
            int process = get_process_number(particle_var.y, size, M);
            MPI_Send(&particle_var, 1, MPI_PARTICLE, process, 0, MPI_COMM_WORLD);
            particleCount[process]++;
        }
        fclose(stdin);
    }
    MPI_Bcast(&particleCount[0], size, MPI_INT, 0, MPI_COMM_WORLD);
    int num_particles = particleCount[rank];
    particles.resize(num_particles);
    for (int i = 0; i < num_particles; i++)
    {
        MPI_Recv(&particles[i], 1, MPI_PARTICLE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    pair<int, int> row_range = get_row_range(rank, size, M);
    int start_row = row_range.first;
    int end_row = row_range.second;
    int root = 0;
    MPI_Barrier(MPI_COMM_WORLD);
    for (int i = 0; i < T; i++)
    {
        for (auto &particle_var : particles)
        {
            if (particle_var.x == 0 && particle_var.dir == 'U')
            {
                particle_var.x = N;
            }
            else if (particle_var.x == N - 1 && particle_var.dir == 'D')
            {
                particle_var.x = -1;
            }
            else if (particle_var.y == 0 && particle_var.dir == 'L')
            {
                particle_var.y = M;
            }
            else if (particle_var.y == M - 1 && particle_var.dir == 'R')
            {
                particle_var.y = -1;
            }
        }
        MPI_Barrier(MPI_COMM_WORLD);
        for (auto &particle_var : particles)
        {
            MPI_Request request;
            if (particle_var.y == -1 && particle_var.dir == 'R')
            {
                int proc_num = get_process_number(0, size, M);
                MPI_Send(&particle_var, 1, MPI_PARTICLE, proc_num, 0, MPI_COMM_WORLD);
                particle_var.x = -1;
                particle_var.y = -1;
            }
            else if (particle_var.y == M && particle_var.dir == 'L')
            {
                int proc_num = get_process_number(M - 1, size, M);
                MPI_Send(&particle_var, 1, MPI_PARTICLE, proc_num, 0, MPI_COMM_WORLD);
                particle_var.x = -1;
                particle_var.y = -1;
            }
            else if (particle_var.y == start_row && (rank - 1) >= 0 && particle_var.dir == 'L')
            {
                MPI_Send(&particle_var, 1, MPI_PARTICLE, rank - 1, 0, MPI_COMM_WORLD);
                particle_var.x = -1;
                particle_var.y = -1;
            }
            else if (particle_var.y == end_row && (rank + 1) < size && particle_var.dir == 'R')
            {
                MPI_Send(&particle_var, 1, MPI_PARTICLE, rank + 1, 0, MPI_COMM_WORLD);
                particle_var.x = -1;
                particle_var.y = -1;
            }
        }
        particles.erase(std::remove_if(particles.begin(), particles.end(), shouldRemoveParticle), particles.end());
        MPI_Barrier(MPI_COMM_WORLD);
        if (rank + 1 < size)
        {
            MPI_Status status;
            MPI_Request request;
            int flag = 0;
            MPI_Iprobe(rank + 1, 0, MPI_COMM_WORLD, &flag, &status);
            while (flag)
            {
                particle p;
                MPI_Recv(&p, 1, MPI_PARTICLE, rank + 1, 0, MPI_COMM_WORLD, &status);
                particles.push_back(p);
                MPI_Iprobe(rank + 1, 0, MPI_COMM_WORLD, &flag, &status);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);

        if (rank - 1 >= 0)
        {
            MPI_Status status;
            MPI_Request request;
            int flag = 0;
            MPI_Iprobe(rank - 1, 0, MPI_COMM_WORLD, &flag, &status);
            while (flag)
            {
                particle p;
                MPI_Recv(&p, 1, MPI_PARTICLE, rank - 1, 0, MPI_COMM_WORLD, &status);
                particles.push_back(p);
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
                particle p;
                MPI_Recv(&p, 1, MPI_PARTICLE, last_process_allocated, 0, MPI_COMM_WORLD, &status);
                particles.push_back(p);
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
                particle p;
                MPI_Recv(&p, 1, MPI_PARTICLE, first_process_allocated, 0, MPI_COMM_WORLD, &status);
                particles.push_back(p);
                MPI_Iprobe(first_process_allocated, 0, MPI_COMM_WORLD, &flag, &status);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);
        for (auto &particle_var : particles)
        {
            int x = particle_var.x;
            int y = particle_var.y;
            if (particle_var.dir == 'L')
            {
                y--;
            }
            else if (particle_var.dir == 'R')
            {
                y++;
            }
            else if (particle_var.dir == 'U')
            {
                x--;
            }
            else if (particle_var.dir == 'D')
            {
                x++;
            }
            particle_var.x = x;
            particle_var.y = y;
        }
        MPI_Barrier(MPI_COMM_WORLD);
        collisionHandle(particles);
        MPI_Barrier(MPI_COMM_WORLD);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    vector<particle> allParticles;
    int total_particles = 0;
    int particle_size = particles.size();
    vector<int> recvcounts(size);
    vector<int> displs(size);
    MPI_Gather(&particle_size, 1, MPI_INT, &recvcounts[0], 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (rank == 0)
    {
        total_particles = accumulate(recvcounts.begin(), recvcounts.end(), 0);
        allParticles.resize(total_particles);
        displs[0] = 0;
        for (int i = 1; i < size; i++)
        {
            displs[i] = displs[i - 1] + recvcounts[i - 1];
        }
    }
    MPI_Gatherv(&particles[0], particles.size(), MPI_PARTICLE, &allParticles[0], &recvcounts[0], &displs[0], MPI_PARTICLE, 0, MPI_COMM_WORLD);
    if (rank == 0)
    {
        freopen(output_file.c_str(), "w", stdout);
        sort(allParticles.begin(), allParticles.end(), compareParticles);
        for (auto &particle_var : allParticles)
        {
            cout << particle_var.x << " " << particle_var.y << " " << particle_var.dir << endl;
        }
        fclose(stdout);
    }
    MPI_Finalize();
    return 0;
}