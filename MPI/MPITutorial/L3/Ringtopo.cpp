#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);
    int size, rank;
    srand(time(NULL));
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    enum role_ranks
    {
        SENDER,
        RECEIVER
    };
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int token = rank;

    if (rank == 0)
    {
        int sum;
        MPI_Send(&token, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD);
        MPI_Recv(&sum, 1, MPI_INT, size - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("Token from %d to %d \n",size-1, rank);
    }
    else
    {
        int recvToken;
        MPI_Recv(&recvToken, 1, MPI_INT, rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("Token from %d to %d \n", rank-1, rank);
        token += recvToken;
        MPI_Send(&token, 1, MPI_INT, (rank + 1) % size, 0, MPI_COMM_WORLD);
    }

    printf("I am %d and I received %d from %d \n", rank, token, (rank - 1 + size) % size);

    MPI_Finalize();
    return EXIT_SUCCESS;
}
