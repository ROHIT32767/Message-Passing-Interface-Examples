#include <mpi.h>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <map>
#include <sstream>

#define CHUNK_SIZE 32
#define REPLICATION_FACTOR 3
#define MAX_FILE_NAME_SIZE 100
#define UPLOAD_TAG 1
#define RETRIEVE_TAG 2
#define SEARCH_TAG 3
#define FAILOVER_TAG 4
#define RECOVER_TAG 5
#define EXIT_TAG 6

using namespace std;

struct ChunkMetaData{
    int chunk_id;
    vector<int> replica_node_ranks;
};

struct FileMetaData{
    string file_name;
    vector<ChunkMetaData> chunks;
};

struct Body{
    int request_type;
    char file_name[MAX_FILE_NAME_SIZE];
    int chunk_id;
    char data[CHUNK_SIZE];
};

struct Chunk{
    int chunk_id;
    string data;
};

vector<int> getReplicaNodeRanks(int chunk_id, int N){
    return {chunk_id % N, (chunk_id + 1) % N, (chunk_id + 2) % N};
}


int main(int argc, char **argv)
{
    int size, rank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int blocklength = sizeof(Body);
    MPI_Datatype MPI_BODY;
    MPI_Type_contiguous(blocklength, MPI_BYTE, &MPI_BODY);
    MPI_Type_commit(&MPI_BODY);
    if (rank == 0)
    {
        // Master Metadata Server
        map<string, FileMetaData> files;
        int N = size;
        string command;
        while (getline(cin, command))
        {
            stringstream ss(command);
            ss >> command;
            if (command == "upload")
            {
                string file_name;
                ss >> file_name;
                string relative_path;
                ss >> relative_path;
                ifstream file(relative_path);
                if (!file)
                {
                    cout << "File not found" << endl;
                    continue;
                }
                string buffer;
                buffer.resize(CHUNK_SIZE);
                int chunk_id = 0;
                while (file.read(&buffer[0], CHUNK_SIZE) || file.gcount() > 0)
                {
                    vector<int> replica_node_ranks = getReplicaNodeRanks(chunk_id, N);
                    string chunk_data = buffer.substr(0, file.gcount());
                    Body body;
                    body.request_type = UPLOAD_TAG;
                    body.file_name = file_name;
                    body.chunk_id = chunk_id;
                    body.data = chunk_data;
                    for(int i=0;i<replica_node_ranks.size();i++){
                        MPI_Send(&body, 1, MPI_BODY, replica_node_ranks[i], UPLOAD_TAG, MPI_COMM_WORLD);
                    }
                    ChunkMetaData chunk_metadata;
                    chunk_metadata.chunk_id = chunk_id;
                    chunk_metadata.replica_node_ranks = replica_node_ranks;
                    files[file_name].chunks.push_back(chunk_metadata);
                    chunk_id++;
                }
            }
            else if(command == "list_file"){

            }
            else if(command == "retrieve"){
                string file_name;
                ss >> file_name;
                FileMetaData file_metadata = files[file_name];
                for(int i=0;i<file_metadata.chunks.size();i++){
                    ChunkMetaData chunk_metadata = file_metadata.chunks[i];
                    Body body;
                    body.request_type = RETRIEVE_TAG;
                    body.file_name = file_name;
                    body.chunk_id = chunk_metadata.chunk_id;
                    int rank_to_send = chunk_metadata.replica_node_ranks[0];
                    MPI_Send(&body, 1, MPI_BODY, rank_to_send, RETRIEVE_TAG, MPI_COMM_WORLD);
                }
                for(int i=0;i<file_metadata.chunks.size();i++){
                    Body body;
                    int rank_to_send = file_metadata.chunks[i].replica_node_ranks[0];
                    MPI_Recv(&body, 1, MPI_BODY, rank_to_send, RETRIEVE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    cout<<body.data;
                }
            }
            else if(command == "search"){

            }
            else if(command == "failover"){
                
            }
            else if(command == "recover"){

            }
            else if(command == "exit"){
                break;
            }
        }
    }
    else
    {
        map<string, vector<Chunk>> storage;
        while(true){
            Body body;
            MPI_Status status;
            MPI_Recv(&body, 1, MPI_BODY, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            if(status.MPI_TAG == UPLOAD_TAG){
                Chunk chunk;
                chunk.chunk_id = body.chunk_id;
                chunk.data = body.data;
                storage[body.file_name].push_back(chunk);
            }
            else if(status.MPI_TAG == RETRIEVE_TAG){
                Chunk chunk = storage[body.file_name][body.chunk_id];
                Body body;
                body.request_type = RETRIEVE_TAG;
                body.file_name = body.file_name;
                body.chunk_id = body.chunk_id;
                body.data = chunk.data;
                MPI_Send(&body, 1, MPI_BODY, 0, RETRIEVE_TAG, MPI_COMM_WORLD);
            }
            else if(status.MPI_TAG == SEARCH_TAG){
                
            }
            else if(status.MPI_TAG == FAILOVER_TAG){
                
            }
            else if(status.MPI_TAG == RECOVER_TAG){
                
            }
            else if(status.MPI_TAG == EXIT_TAG){
                break;
            }
        }
    }
    MPI_Finalize();
    return 0;
}