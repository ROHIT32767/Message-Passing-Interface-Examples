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

MPI_Datatype MPI_PARTICLE;
bool opposite_direction(char c1, char c2)
{
    if ((c1 == 'U' && c2 == 'D') or (c1 == 'D' && c2 == 'U') or (c1 == 'L' && c2 == 'R') or (c1 == 'R' && c2 == 'L'))
        return true;

    return false;
}

void changeDirectionandCoordinate(particle &p, int iterations)
{
    while (iterations--)
    {
        char dir = p.dir;
        if (dir == 'R')
            dir = 'U';
        else if (dir == 'U')
            dir = 'L';
        else if (dir == 'L')
            dir = 'D';
        else if (dir == 'D')
            dir = 'R';
        p.dir = dir;
    }
}

void collisionHandle(vector<particle> &particles)
{
    map<pair<int, int>, int> collisions;
    map<int, int> mp;
    for (int i = 0; i < particles.size(); i++)
        mp[particles[i].ind] = i;

    map< pair<int, int>, set<int> > indexParticles;
    for (int i = 0; i < particles.size(); i++)
    {
        indexParticles[{particles[i].x, particles[i].y}].insert(particles[i].ind);
    }
    for (auto &p : particles)
    {
        if (indexParticles[{p.x, p.y}].size() == 2)
        {
            auto it = indexParticles[{p.x, p.y}].begin();
            int ind11 = *it;
            it++;
            int ind22 = *it;
            int ind1 = -1, ind2 = -1;

           ind1 = mp[ind11];
           ind2 = mp[ind22];

            if (ind1 != -1 and ind2 != -1 and opposite_direction(particles[ind1].dir, particles[ind2].dir) and collisions[{ind1, ind2}]!=-1 and collisions[{ind2, ind1}]!=-1)
            {
                collisions[{ind1, ind2}] = -1;
                collisions[{ind2, ind1}] = -1;
                if (particles[ind1].dir == 'U')
                {
                    particles[ind2].dir = (particles[ind2].dir == 'D') ? 'L' : 'R';
                    particles[ind1].dir = 'R';
                }
                else if (particles[ind1].dir == 'D')
                {
                    particles[ind2].dir = (particles[ind2].dir == 'U') ? 'R' : 'L';
                    particles[ind1].dir = 'L';
                }
                else if (particles[ind1].dir == 'L')
                {
                    particles[ind2].dir = (particles[ind2].dir == 'R') ? 'D' : 'U';
                    particles[ind1].dir = 'U';
                }
                else if (particles[ind1].dir == 'R')
                {
                    particles[ind2].dir = (particles[ind2].dir == 'L') ? 'U' : 'D';
                    particles[ind1].dir = 'D';
                }
            }
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
}

int main(int argc, char *argv[])
{
    int n, m, numParticles, numTimeSteps;
    MPI_Init(&argc, &argv);
    int rank, num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    int type = 0;
    if (rank == 0)
    {
        cin >> n >> m >> numParticles >> numTimeSteps;
        if (n > m)
        {
            type = 1;
            swap(n, m);
        }
    }
    MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&m, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&numParticles, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&numTimeSteps, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&type, 1, MPI_INT, 0, MPI_COMM_WORLD);
    int blockLength = sizeof(particle);
    MPI_Type_contiguous(blockLength, MPI_BYTE, &MPI_PARTICLE);
    MPI_Type_commit(&MPI_PARTICLE);
    // double start = MPI_Wtime();
    vector<particle> particles(numParticles);
    vector<int> particleCount(num_procs, 0);
    if (rank == 0)
    {
        for (int i = 0; i < numParticles; i++)
        {
            int x, y;
            char dir;
            cin >> x >> y >> dir;
            particles[i].ind = i;
            particles[i].x = x;
            particles[i].y = y;
            particles[i].dir = dir;
        }

        if (type == 1)
        {
            for (auto &p : particles)
            {
                int x = p.y;
                int y = (m - 1) - p.x;
                p.x = x;
                p.y = y;
                changeDirectionandCoordinate(p, 3);
            }
        }

        for (auto &p : particles)
        {
            int x = p.x;
            int y = p.y;
            int pr = (m + num_procs - 1) / num_procs;
            int process = (y + pr - 1) / pr;
            if (process == num_procs)
                process--;
            MPI_Send(&p, 1, MPI_PARTICLE, process, 0, MPI_COMM_WORLD);
            particleCount[process]++;
        }
    }
    MPI_Bcast(&particleCount[0], num_procs, MPI_INT, 0, MPI_COMM_WORLD);
    int num = particleCount[rank];
    particles.resize(num);
    for (int i = 0; i < num; ++i)
    {
        MPI_Recv(&particles[i], 1, MPI_PARTICLE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
    int rowPerProcess = (m + num_procs - 1) / num_procs;
    int startRow, endRow;
    if (rank == 0)
        startRow = endRow = 0;
    else
    {
        startRow = (rank - 1) * rowPerProcess + 1;
        endRow = (rank)*rowPerProcess;
    }

    int root = 0;
    MPI_Barrier(MPI_COMM_WORLD);
    collisionHandle(particles);

    for (int i = 0; i < numTimeSteps; i++)
    {
        for (auto &p : particles)
        {
            if (p.x == 0 and p.dir == 'L')
                p.dir = 'R';
            else if (p.x == n - 1 and p.dir == 'R')
                p.dir = 'L';
            else if (p.y == 0 and p.dir == 'D')
                p.dir = 'U';
            else if (p.y == m - 1 and p.dir == 'U')
                p.dir = 'D';
        }
        MPI_Barrier(MPI_COMM_WORLD);

        for (auto &p : particles)
        {
            MPI_Request request;
            if (p.y == startRow and (rank - 1) >= 0 and p.dir == 'D')
            {
                MPI_Send(&p, 1, MPI_PARTICLE, rank - 1, 0, MPI_COMM_WORLD);
                p.x = -1;
                p.y = -1;
            }

            if (p.y == endRow and (rank + 1) < num_procs and p.dir == 'U')
            {
                MPI_Send(&p, 1, MPI_PARTICLE, rank + 1, 0, MPI_COMM_WORLD);
                p.x = -1;
                p.y = -1;
            }
        }
        particles.erase(std::remove_if(particles.begin(), particles.end(), [](const particle &p)
                                       { return p.x == -1 and p.y == -1; }),
                        particles.end());

       
        MPI_Barrier(MPI_COMM_WORLD);
        if (rank + 1 < num_procs)
        {
            MPI_Status status;
            MPI_Request request;
            int flag = 0;
            MPI_Iprobe(rank + 1, 0, MPI_COMM_WORLD, &flag, &status);
            while (flag == 1)
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
            while (flag == 1)
            {
                particle p;
                MPI_Recv(&p, 1, MPI_PARTICLE, rank - 1, 0, MPI_COMM_WORLD, &status);
                particles.push_back(p);
                MPI_Iprobe(rank - 1, 0, MPI_COMM_WORLD, &flag, &status);
            }
        }
        MPI_Barrier(MPI_COMM_WORLD);
        for (auto &p : particles)
        {
            int x = p.x;
            int y = p.y;

            if (p.dir == 'L')
                x--;
            else if (p.dir == 'R')
                x++;
            else if (p.dir == 'U')
                y++;
            else if (p.dir == 'D')
                y--;

            p.x = x;
            p.y = y;
        }

        MPI_Barrier(MPI_COMM_WORLD);
        collisionHandle(particles);
        MPI_Barrier(MPI_COMM_WORLD);

    }
    MPI_Barrier(MPI_COMM_WORLD);
    vector<particle> allParticles;
    int totParticles = 0;
    int size = particles.size();
    vector<int> recvcounts(num_procs);
    vector<int> displs(num_procs);

    MPI_Gather(&size, 1, MPI_INT, &recvcounts[0], 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank == 0)
    {
        totParticles = accumulate(recvcounts.begin(), recvcounts.end(), 0);
        if (totParticles == 0)
        {
            cout << "No particles present" << endl;
            return 0;
        }
        allParticles.resize(totParticles);
        displs[0] = 0;
        for (int i = 1; i < num_procs; i++)
        {
            displs[i] = displs[i - 1] + recvcounts[i - 1];
        }
    }
    MPI_Gatherv(&particles[0], size, MPI_PARTICLE, &allParticles[0], &recvcounts[0], &displs[0], MPI_PARTICLE, 0, MPI_COMM_WORLD);

    if (rank == 0)
    {
        // ofstream fout("output.txt");
        if (type == 1)
        {
            for (auto &p : allParticles)
            {
                int x = (m - 1) - p.y;
                int y = p.x;
                p.x = x;
                p.y = y;
                changeDirectionandCoordinate(p, 1);
            }
        }
        sort(allParticles.begin(), allParticles.end(), [](const particle &p1, const particle &p2)
             {
                 if (p1.x == p2.x)
                     return p1.y < p2.y;
                 return p1.x < p2.x; });
        for (auto &p : allParticles)
        {
            cout << p.x << " " << p.y << " " << p.dir << endl;
        }
    }
    
    MPI_Finalize();
    return 0;
}