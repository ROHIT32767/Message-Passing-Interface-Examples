#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <stdlib.h>
#include <time.h>

#define LEN 100000

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);
    int size, rank;
    srand(time(NULL));
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int token = rank;

    int token_array[LEN];
    for (int i = 0; i < LEN; i++)
    {
        token_array[i] = rand() % 100;
    }


    MPI_Status status;
    int recv_array[LEN];

    MPI_Send(&token,LEN, MPI_INT, (rank + 1) % size, 0, MPI_COMM_WORLD);
    printf("Token from %d to %d \n", rank, (rank + 1) % size);
    MPI_Recv(&recv_array, LEN, MPI_INT, (rank - 1+ size) % size, 0, MPI_COMM_WORLD, &status);

    printf("I am %d and I received %d from %d \n", rank, recv_array[0], (rank - 1+ size) % size);

    MPI_Finalize();


}