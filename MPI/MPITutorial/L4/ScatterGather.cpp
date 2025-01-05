#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <stdlib.h>
#include <time.h>

void swap(int *xp, int *yp)
{
    int temp = *xp;
    *xp = *yp;
    *yp = temp;
}

void bubbleSort(int arr[], int n)
{
    int i, j;
    for (i = 0; i < n - 1; i++)

        // Last i elements are already in place
        for (j = 0; j < n - i - 1; j++)
            if (arr[j] > arr[j + 1])
                swap(&arr[j], &arr[j + 1]);
}

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);
    int size, rank;
    srand(time(NULL));
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (rank == 0 && size != 10) {
        printf("This application is meant to be run with 10 processes.\n");
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }
    int sendSize = 100;
    int chunkSize = 10;
    int recvBuffer[15];
    int bufferUnsorted[105];
    int bufferSorted[105];

    if (rank == 0)
    {
        for (int i = 0; i < 100; i++)
        {
            bufferUnsorted[i] = int(rand() % 101) + 1;
            printf("%d ", bufferUnsorted[i]);
        }
    }
    printf("\n");
    MPI_Scatter(bufferUnsorted, chunkSize, MPI_INT, recvBuffer, chunkSize, MPI_INT, 0, MPI_COMM_WORLD);
    bubbleSort(recvBuffer, 10);
    MPI_Gather(recvBuffer, chunkSize, MPI_INT, bufferSorted, chunkSize, MPI_INT, 0, MPI_COMM_WORLD);
    if (rank == 0)
    {
        printf("\n");
        for (int i = 0; i < 100; i++)
            printf("%d ", bufferSorted[i]);
        printf("\n");
    }

    MPI_Finalize();
    return EXIT_SUCCESS;
}
