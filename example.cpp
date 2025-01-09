
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "mpi.h"
#include <complex.h>
#include <bits/stdc++.h>
#include <iostream>

using namespace std;

double complAbs(complex<double> z)
{
    return sqrt(z.real() * z.real() + z.imag() * z.imag());
}

int mandelBrot(double x, double y, int maxIterations)
{
    complex<double> z(0, 0);
    complex<double> c(x, y);
    int iterations = 0;
    while (iterations < maxIterations)
    {
        z = z * z + c;
        iterations++;
    }
    return (complAbs(z) < 2) ? 1 : 0;
    // return iterations;
}

int main(int argc, char *argv[])
{

    MPI_Init(&argc, &argv);
    int my_rank, comm_sz;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    MPI_Barrier(MPI_COMM_WORLD);
    double start = MPI_Wtime();

    // ifstream fin("input.txt");
    int N, M, K;
    if (my_rank == 0)
    {
        cin >> N >> M >> K;
    }
    MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&M, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&K, 1, MPI_INT, 0, MPI_COMM_WORLD);


    int rows_per_process = (N * M) / comm_sz;
    int start_row = my_rank * rows_per_process;
    int end_row = start_row + rows_per_process;
    if (my_rank == comm_sz - 1)
    {
        end_row = N*M;
    }
    // cout << "rank: " << my_rank << " start: " << start_row << " end: " << end_row << endl;
    int tot_size = (end_row - start_row);
    
    vector<int> rows_data;
    rows_data.resize(tot_size);
    for (int i = start_row; i < end_row; i++)
    {
        int row = i / (M);
        int col = i % (M);
        double real = (double)col / (M - 1) * 2.5 - 1.5;
        double imag = (double)row / (N - 1) * 2 - 1;
        rows_data[row*M - start_row + col] = mandelBrot(real, imag, K);
    }

    vector<int> final_data;
    if(my_rank==0)final_data.resize(N*M);

    vector<int> recvcounts(comm_sz);
    vector<int> displs(comm_sz,0);
    MPI_Gather(&tot_size, 1, MPI_INT, &recvcounts[0], 1, MPI_INT, 0, MPI_COMM_WORLD);

    if(my_rank == 0){
        for (int i = 1; i < comm_sz; i++)
             displs[i] = displs[i - 1] + recvcounts[i - 1];
    }

    MPI_Gatherv(&rows_data[0], tot_size, MPI_INT, &final_data[0], &recvcounts[0], &displs[0], MPI_INT, 0, MPI_COMM_WORLD);
    
    if (my_rank == 0)
    {

        for (int i = 0; i < N; i++)
        {
            for (int j = 0; j < M; j++)
            {
                cout << final_data[i * M + j] << " ";
            }
            cout << "\n";
        }
    }
    rows_data.clear();
    final_data.clear();

    MPI_Finalize();
    return 0;
}
