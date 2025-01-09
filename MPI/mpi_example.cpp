/**
 * @author RookieHPC
 * @brief Original source code at https://rookiehpc.org/mpi/docs/mpi_comm_world/index.html
 **/

#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

/**
 * @brief For each process in the default communicator MPI_COMM_WORLD, show their rank.
 **/
int main(int argc, char* argv[])
{
    MPI_Init(&argc, &argv);

    int my_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    int my_size;
    MPI_Comm_size(MPI_COMM_WORLD, &my_size);

    printf("I am MPI process %d.\n", my_rank);
    printf("There are %d processes in the default communicator.\n", my_size);

    MPI_Finalize();

    return EXIT_SUCCESS;
}
