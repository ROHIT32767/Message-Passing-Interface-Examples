#include <mpi.h>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>

#define CHUNK_SIZE 32
#define REPLICATION_FACTOR 3

using namespace std;

struct ChunkInfo {
    int chunk_id;
    vector<int> nodes; // Nodes storing this chunk
};

unordered_map<string, vector<ChunkInfo>> metadata;

void metadata_server(int world_size) {
    char command[256];

    while (true) {
        MPI_Status status;
        MPI_Recv(command, 256, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

        string cmd(command);
        if (cmd.find("upload") == 0) {
            // Upload a file
            string file_name = cmd.substr(7, cmd.find(" ", 7) - 7);
            string file_path = cmd.substr(cmd.find(" ", 7) + 1);

            ifstream file(file_path, ios::binary);
            if (!file) {
                cerr << "Error opening file: " << file_path << endl;
                continue;
            }

            vector<ChunkInfo> chunks;
            int chunk_id = 0;
            int node = 1; // Start distributing from rank 1
            char buffer[CHUNK_SIZE];

            while (file.read(buffer, CHUNK_SIZE) || file.gcount() > 0) {
                int bytes_read = file.gcount();

                vector<int> nodes;
                for (int i = 0; i < REPLICATION_FACTOR; ++i) {
                    nodes.push_back(node);
                    node = (node % (world_size - 1)) + 1; // Round-robin distribution
                }

                for (int n : nodes) {
                    MPI_Send(buffer, bytes_read, MPI_CHAR, n, chunk_id, MPI_COMM_WORLD);
                }

                chunks.push_back({chunk_id, nodes});
                ++chunk_id;
            }

            metadata[file_name] = chunks;
            cout << "File uploaded: " << file_name << endl;

        } else if (cmd.find("retrieve") == 0) {
            // Retrieve a file
            string file_name = cmd.substr(9);

            if (metadata.find(file_name) == metadata.end()) {
                cout << "-1" << endl;
                continue;
            }

            vector<ChunkInfo> chunks = metadata[file_name];
            ofstream output_file(file_name, ios::binary);

            for (const ChunkInfo &chunk : chunks) {
                char buffer[CHUNK_SIZE];
                MPI_Status status;
                MPI_Recv(buffer, CHUNK_SIZE, MPI_CHAR, chunk.nodes[0], chunk.chunk_id, MPI_COMM_WORLD, &status);
                output_file.write(buffer, status._ucount);
            }

            output_file.close();
            cout << "File retrieved: " << file_name << endl;

        } else if (cmd.find("search") == 0) {
            // Search for a word
            string file_name = cmd.substr(7, cmd.find(" ", 7) - 7);
            string word = cmd.substr(cmd.find(" ", 7) + 1);

            if (metadata.find(file_name) == metadata.end()) {
                cout << "-1" << endl;
                continue;
            }

            vector<ChunkInfo> chunks = metadata[file_name];
            bool found = false;

            for (const ChunkInfo &chunk : chunks) {
                for (int node : chunk.nodes) {
                    MPI_Send(word.c_str(), word.size() + 1, MPI_CHAR, node, chunk.chunk_id, MPI_COMM_WORLD);
                    int result;
                    MPI_Recv(&result, 1, MPI_INT, node, chunk.chunk_id, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                    if (result == 1) {
                        found = true;
                        break;
                    }
                }
                if (found) break;
            }

            cout << (found ? "Word found" : "-1") << endl;

        } else if (cmd.find("list_file") == 0) {
            // List chunks of a file
            string file_name = cmd.substr(10);

            if (metadata.find(file_name) == metadata.end()) {
                cout << "-1" << endl;
                continue;
            }

            vector<ChunkInfo> chunks = metadata[file_name];
            for (const ChunkInfo &chunk : chunks) {
                cout << "Chunk " << chunk.chunk_id << " stored on nodes: ";
                for (int node : chunk.nodes) cout << node << " ";
                cout << endl;
            }
        }
    }
}

void storage_node(int rank) {
    unordered_map<int, string> chunks;

    while (true) {
        char buffer[CHUNK_SIZE];
        MPI_Status status;
        MPI_Recv(buffer, CHUNK_SIZE, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        int chunk_id = status.MPI_TAG;
        chunks[chunk_id] = string(buffer, status._ucount);
    }
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int world_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    if (world_size < 4) {
        cerr << "At least 4 processes are required." << endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (world_rank == 0) {
        metadata_server(world_size);
    } else {
        storage_node(world_rank);
    }

    MPI_Finalize();
    return 0;
}
