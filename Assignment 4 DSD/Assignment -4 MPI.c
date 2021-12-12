#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>

#define M 32
#define N 20     // Size of resultant matrix            

#define master_tag 1          
#define worker_tag 2          

int main(int argc, char* argv[]) {

	int NUM_PROCESS, rank;
	int rows, portion;
	int remainder, offset;

	int i, j, k;

	int a[N][M], b[M][N], c[N][N];

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &NUM_PROCESS);
	MPI_Status status;


	if (rank == 0) {
		double startTime = MPI_Wtime();

		for (i = 0; i < N; i++)
			for (j = 0; j < M; j++)
				a[i][j] = j + 1;

		for (i = 0; i < M; i++)
			for (j = 0; j < N; j++)
				b[i][j] = j + 1;

		printf("Array 1:\n");
		for (i = 0; i < N; i++) {
			for (j = 0; j < M; j++) {
				printf("%d\t", a[i][j]);
			}printf("\n");
		}


		printf("Array 2:\n");
		for (i = 0; i < M; i++) {
			for (j = 0; j < N; j++) {
				printf("%d\t", b[i][j]);
			}printf("\n");
		}

		portion = N / (NUM_PROCESS - 1);
		remainder = N % (NUM_PROCESS - 1);
		offset = 0;

		for (j = 1; j < NUM_PROCESS; j++) {
			rows = (j <= remainder) ? portion + 1 : portion;
			//rows = (j == NUM_PROCESS - 1) ? remainder : portion;


			MPI_Send(&offset, 1, MPI_INT, j, master_tag, MPI_COMM_WORLD);
			MPI_Send(&rows, 1, MPI_INT, j, master_tag, MPI_COMM_WORLD);
			MPI_Send(&a[offset][0], rows * M, MPI_INT, j, master_tag, MPI_COMM_WORLD);
			MPI_Send(&b, M * N, MPI_INT, j, master_tag, MPI_COMM_WORLD);
			offset = offset + rows;
		}

		//Receive from Worker Threads
		for (i = 1; i < NUM_PROCESS; i++) {

			MPI_Recv(&offset, 1, MPI_INT, i, worker_tag, MPI_COMM_WORLD, &status);
			MPI_Recv(&rows, 1, MPI_INT, i, worker_tag, MPI_COMM_WORLD, &status);
			MPI_Recv(&c[offset][0], rows * N, MPI_INT, i, worker_tag, MPI_COMM_WORLD, &status);
		}

		printf("\n\tThe Resultant Matrix is ::\n");
		for (i = 0; i < N; i++) {
			printf("\n");
			for (j = 0; j < N; j++)
				printf("%d   ", c[i][j]);
		}
		printf("\n");

		double endTime = MPI_Wtime();

		printf("Total time elapsed :: %f\n", (endTime - startTime));
	}

	//Worker Threads
	if (rank > 0) {
		MPI_Recv(&offset, 1, MPI_INT, 0, master_tag, MPI_COMM_WORLD, &status);
		MPI_Recv(&rows, 1, MPI_INT, 0, master_tag, MPI_COMM_WORLD, &status);
		MPI_Recv(&a, rows * M, MPI_INT, 0, master_tag, MPI_COMM_WORLD, &status);
		MPI_Recv(&b, M * N, MPI_INT, 0, master_tag, MPI_COMM_WORLD, &status);

		for (k = 0; k < N; k++)
			for (i = 0; i < rows; i++)
			{
				c[i][k] = 0.0;
				for (j = 0; j < M; j++)
					c[i][k] += a[i][j] * b[j][k];
			}
		MPI_Send(&offset, 1, MPI_INT, 0, worker_tag, MPI_COMM_WORLD);
		MPI_Send(&rows, 1, MPI_INT, 0, worker_tag, MPI_COMM_WORLD);
		MPI_Send(&c, rows * N, MPI_INT, 0, worker_tag, MPI_COMM_WORLD);
	}
	MPI_Finalize();
}