#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <math.h>

int main(int argc, char* argv[])
{
    MPI_Init(&argc, &argv);
    int root_rank = 0;
    int size = 0;
    int my_rank;

    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    int power;
    if(my_rank == root_rank)
	    power = 2;

    MPI_Bcast(&power,1,MPI_INT,root_rank,MPI_COMM_WORLD);

    int my_result = (int)(pow(my_rank, power));

    // MPI_Reduce(&my_result, &reduction_result, 1, MPI_INT, MPI_SUM, root_rank, MPI_COMM_WORLD);
    if(my_rank == root_rank)
    {
        int reduction_result = 0;
        MPI_Reduce(&my_result, &reduction_result, 1, MPI_INT, MPI_SUM, root_rank, MPI_COMM_WORLD);
        // cout << "The sum of powers of all ranks - " << reduction_result << endl;
        printf("Number of Processes - %d.\n", size);
        printf("The sum of powers of all ranks - %d.\n", reduction_result);
    }
    else
    {
        MPI_Reduce(&my_result, NULL, 1, MPI_INT, MPI_SUM, root_rank, MPI_COMM_WORLD);
    }

    // if(my_rank == root_rank)
    // {
        // printf("Number of Processes - %d.\n", size);
        // printf("The sum of powers of all ranks - %d.\n", reduction_result);
    // }

    MPI_Finalize();
    return EXIT_SUCCESS;
}

