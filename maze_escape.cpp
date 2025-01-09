#include <mpi.h>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <limits>
#include <set>

int main(int argc, char* argv[]){
    MPI_Init(&argc, &argv);
    int rank;
    int size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int R,V,E,K,L;
    vector<vector<int>> adj_list(V,vector<int>());
    vector<int> blocked_nodes(L);
    vector<int> starting_nodes(K);
    if(rank==0){
        cin >> V >> E;
        for(int i=0;i<E;i++){
            int u,v,d;
            cin >> u >> v >> d;
            if(d==1){
                adj_list[u].push_back(v);
                adj_list[v].push_back(u);
            }
            else{
                adj_list[u].push_back(v);
            }
        }
        cin >> K;
        for(int i=0;i<K;i++){
            int x;
            cin >> x;
            starting_nodes[i]=x;
        }
        cin >> R;
        cin >> L;
        for(int i=0;i<L;i++){
            int x;
            cin >> x;
            blocked_nodes[i]=x;
        }
    }
    MPI_Bcast(&V,1,MPI_INT,0,MPI_COMM_WORLD);
    MPI_Bcast(&E,1,MPI_INT,0,MPI_COMM_WORLD);
    MPI_Bcast(&R,1,MPI_INT,0,MPI_COMM_WORLD);

    

    // Distribute the graph data wrt vertices to all processes
    
    int vertices_per_process = V/size;
    int start_vertex = rank*vertices_per_process;
    int end_vertex = start_vertex + vertices_per_process;

    if(rank==size-1){
        end_vertex = V;
    }

    vector<vector<int>> local_adj_list(V,vector<int>());
    for(int i=start_vertex;i<end_vertex;i++){
        local_adj_list[i] = adj_list[i];
    }

    
}