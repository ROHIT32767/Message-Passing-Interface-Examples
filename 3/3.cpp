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
#define UPLOAD_TAG 1
#define RETRIEVE_TAG 2
#define SEARCH_TAG 3
#define FAILOVER_TAG 4
#define RECOVER_TAG 5
#define EXIT_TAG 6

using namespace std;

struct ChunkMetaData
{
    int chunk_id;
    vector<int> replica_node_ranks;
};

struct FileMetaData
{
    string file_name;
    vector<ChunkMetaData> chunks;
};

struct Body
{
    int request_type;
    int chunk_id;
};

struct Chunk
{
    int chunk_id;
    string data;
};

vector<int> getReplicaNodeRanks(int chunk_id, int N)
{
    return {
        1 + (chunk_id % (N - 1)),
        1 + ((chunk_id + 1) % (N - 1)),
        1 + ((chunk_id + 2) % (N - 1))};
}

MPI_Datatype MPI_BODY;

int main(int argc, char **argv)
{
    int size, rank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int blocklength = sizeof(Body);
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
                    cout << -1 << endl;
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
                    body.chunk_id = chunk_id;
                    for (int i = 0; i < replica_node_ranks.size(); i++)
                    {
                        MPI_Send(&body, 1, MPI_BODY, replica_node_ranks[i], UPLOAD_TAG, MPI_COMM_WORLD);
                        int file_name_size = file_name.size();
                        MPI_Send(&file_name_size, 1, MPI_INT, replica_node_ranks[i], UPLOAD_TAG, MPI_COMM_WORLD);
                        MPI_Send(file_name.c_str(), file_name_size, MPI_CHAR, replica_node_ranks[i], UPLOAD_TAG, MPI_COMM_WORLD);
                        int chunk_data_size = chunk_data.size();
                        MPI_Send(&chunk_data_size, 1, MPI_INT, replica_node_ranks[i], UPLOAD_TAG, MPI_COMM_WORLD);
                        MPI_Send(chunk_data.c_str(), chunk_data_size, MPI_CHAR, replica_node_ranks[i], UPLOAD_TAG, MPI_COMM_WORLD);
                    }
                    ChunkMetaData chunk_metadata;
                    chunk_metadata.chunk_id = chunk_id;
                    chunk_metadata.replica_node_ranks = replica_node_ranks;
                    files[file_name].chunks.push_back(chunk_metadata);
                    chunk_id++;
                }
                cout << 1 << endl;
                for (int i = 0; i < files[file_name].chunks.size(); i++)
                {
                    cout << files[file_name].chunks[i].chunk_id << " " << files[file_name].chunks[i].replica_node_ranks.size() << " ";
                    for (int j = 0; j < files[file_name].chunks[i].replica_node_ranks.size(); j++)
                    {
                        cout << files[file_name].chunks[i].replica_node_ranks[j] << " ";
                    }
                    cout << endl;
                }
            }
            else if (command == "list_file")
            {
                string file_name;
                ss >> file_name;
                for (int i = 0; i < files[file_name].chunks.size(); i++)
                {
                    cout << files[file_name].chunks[i].chunk_id << " " << files[file_name].chunks[i].replica_node_ranks.size() << " ";
                    for (int j = 0; j < files[file_name].chunks[i].replica_node_ranks.size(); j++)
                    {
                        cout << files[file_name].chunks[i].replica_node_ranks[j] << " ";
                    }
                    cout << endl;
                }
            }
            else if (command == "retrieve")
            {
                string file_name;
                ss >> file_name;
                FileMetaData file_metadata = files[file_name];
                vector<pair<int, string>> received_chunks;
                for (int i = 0; i < file_metadata.chunks.size(); i++)
                {
                    ChunkMetaData chunk_metadata = file_metadata.chunks[i];
                    Body body;
                    body.request_type = RETRIEVE_TAG;
                    body.chunk_id = chunk_metadata.chunk_id;
                    int rank_to_send = chunk_metadata.replica_node_ranks[0];
                    MPI_Send(&body, 1, MPI_BODY, rank_to_send, RETRIEVE_TAG, MPI_COMM_WORLD);
                    int file_name_size = file_name.size();
                    MPI_Send(&file_name_size, 1, MPI_INT, rank_to_send, RETRIEVE_TAG, MPI_COMM_WORLD);
                    MPI_Send(file_name.c_str(), file_name_size, MPI_CHAR, rank_to_send, RETRIEVE_TAG, MPI_COMM_WORLD);
                }
                for (int i = 0; i < file_metadata.chunks.size(); i++)
                {
                    int chunk_size;
                    MPI_Status status;
                    int rank_to_receive_from = file_metadata.chunks[i].replica_node_ranks[0];
                    MPI_Recv(&chunk_size, 1, MPI_INT, rank_to_receive_from, RETRIEVE_TAG, MPI_COMM_WORLD, &status);
                    string chunk_data;
                    chunk_data.resize(chunk_size);
                    MPI_Recv(&chunk_data[0], chunk_size, MPI_CHAR, rank_to_receive_from, RETRIEVE_TAG, MPI_COMM_WORLD, &status);
                    received_chunks.emplace_back(file_metadata.chunks[i].chunk_id, chunk_data);
                }
                sort(received_chunks.begin(), received_chunks.end());
                for (const auto &chunk : received_chunks)
                {
                    cout << chunk.second;
                }
                cout << endl;
            }
            else if (command == "search")
            {
            }
            else if (command == "failover")
            {
            }
            else if (command == "recover")
            {
            }
            else if (command == "exit")
            {
                for (int i = 1; i < size; i++)
                {
                    Body body;
                    MPI_Send(&body, 1, MPI_BODY, i, EXIT_TAG, MPI_COMM_WORLD);
                }
                break;
            }
        }
    }
    else
    {
        map<string, vector<Chunk>> storage;
        while (true)
        {
            Body body;
            MPI_Status status;
            MPI_Recv(&body, 1, MPI_BODY, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            if (status.MPI_TAG == UPLOAD_TAG)
            {
                Chunk chunk;
                chunk.chunk_id = body.chunk_id;
                int file_name_size;
                MPI_Recv(&file_name_size, 1, MPI_INT, 0, UPLOAD_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                string file_name;
                file_name.resize(file_name_size);
                MPI_Recv(&file_name[0], file_name_size, MPI_CHAR, 0, UPLOAD_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                int chunk_data_size;
                MPI_Recv(&chunk_data_size, 1, MPI_INT, 0, UPLOAD_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                string chunk_data;
                chunk_data.resize(chunk_data_size);
                MPI_Recv(&chunk_data[0], chunk_data_size, MPI_CHAR, 0, UPLOAD_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                chunk.data = chunk_data;
                storage[file_name].push_back(chunk);
            }
            else if (status.MPI_TAG == RETRIEVE_TAG)
            {
                string file_name;
                int file_name_size;
                MPI_Recv(&file_name_size, 1, MPI_INT, 0, RETRIEVE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                file_name.resize(file_name_size);
                MPI_Recv(&file_name[0], file_name_size, MPI_CHAR, 0, RETRIEVE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                int chunk_id = body.chunk_id;
                for (int i = 0; i < storage[file_name].size(); i++)
                {
                    if (storage[file_name][i].chunk_id == chunk_id)
                    {
                        int chunk_data_size = storage[file_name][i].data.size();
                        MPI_Send(&chunk_data_size, 1, MPI_INT, 0, RETRIEVE_TAG, MPI_COMM_WORLD);
                        MPI_Send(storage[file_name][i].data.c_str(), chunk_data_size, MPI_CHAR, 0, RETRIEVE_TAG, MPI_COMM_WORLD);
                    }
                }
            }
            else if (status.MPI_TAG == SEARCH_TAG)
            {
            }
            else if (status.MPI_TAG == FAILOVER_TAG)
            {
            }
            else if (status.MPI_TAG == RECOVER_TAG)
            {
            }
            else if (status.MPI_TAG == EXIT_TAG)
            {
                break;
            }
        }
    }
    MPI_Finalize();
    return 0;
}