#include <mpi.h>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <map>
#include <sstream>
#include <set>

#include <thread>
#include <mutex>
#include <chrono>
#include <atomic>
#include <filesystem>

#define CHUNK_SIZE 32
#define REPLICATION_FACTOR 3
#define UPLOAD_TAG 1
#define RETRIEVE_TAG 2
#define SEARCH_TAG 3
#define FAILOVER_TAG 4
#define RECOVER_TAG 5
#define EXIT_TAG 6
#define HEARTBEAT_TAG 7

using namespace std;

MPI_Datatype MPI_BODY;

struct Compare
{
    bool operator()(const pair<int, int> &a, const pair<int, int> &b) const
    {
        return a.first < b.first || (a.first == b.first && a.second < b.second);
    }
};

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
    vector<int> offsets;
};

struct Body
{
    int request_type;
    int chunk_id;
    int sender_rank;
    bool stop;
};

struct Chunk
{
    int chunk_id;
    string data;
};

vector<int> getReplicaNodeRanks(int chunk_id, int N, set<int> &failed_nodes)
{
    set<int> valid_nodes;
    for (int i = 1; i < N; ++i)
    {
        valid_nodes.insert(i);
    }
    for (int failed_node : failed_nodes)
    {
        valid_nodes.erase(failed_node);
    }
    vector<int> replica_nodes;
    auto it = valid_nodes.begin();
    for (int i = 0; i < 3; ++i)
    {
        if (it != valid_nodes.end())
        {
            replica_nodes.push_back(*it);
            ++it;
        }
        else
        {
            replica_nodes.push_back(-1);
        }
    }
    return replica_nodes;
}

void increment_size(std::multiset<std::pair<int, int>, Compare> &chunk_size_set, int node)
{
    auto it = std::find_if(chunk_size_set.begin(), chunk_size_set.end(), [node](const std::pair<int, int> &p)
                           { return p.second == node; });
    if (it != chunk_size_set.end())
    {
        std::pair<int, int> updated = *it;
        chunk_size_set.erase(it);
        updated.first += 1;
        chunk_size_set.insert(updated);
    }
    else
    {
        chunk_size_set.insert({1, node});
    }
}

vector<int> get_replicate_node_ranks(multiset<pair<int, int>, Compare> &chunk_size_set, int N, set<int> &failed_nodes)
{
    vector<int> replica_nodes;
    auto it = chunk_size_set.begin();
    for (int i = 0; i < 3; ++i)
    {
        while (it != chunk_size_set.end() && failed_nodes.find(it->second) != failed_nodes.end())
        {
            it++;
        }
        if (it != chunk_size_set.end())
        {
            replica_nodes.push_back(it->second);
            it++;
        }
        else
        {
            replica_nodes.push_back(-1);
        }
    }
    return replica_nodes;
}

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
        map<string, FileMetaData> files;
        multiset<pair<int, int>, Compare> chunk_size_set;
        for (int i = 1; i < size; i++)
        {
            chunk_size_set.insert({0, i});
        }
        int N = size;
        string command;
        set<int> failed_nodes;
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
                int offset = 0;
                while (file.read(&buffer[0], CHUNK_SIZE) || file.gcount() > 0)
                {
                    vector<int> replica_node_ranks = get_replicate_node_ranks(chunk_size_set, N, failed_nodes);
                    string chunk_data = buffer.substr(0, file.gcount());
                    Body body;
                    body.request_type = UPLOAD_TAG;
                    body.chunk_id = chunk_id;
                    for (int i = 0; i < replica_node_ranks.size(); i++)
                    {
                        if (replica_node_ranks[i] == -1)
                        {
                            continue;
                        }
                        MPI_Send(&body, 1, MPI_BODY, replica_node_ranks[i], UPLOAD_TAG, MPI_COMM_WORLD);
                        int file_name_size = file_name.size();
                        MPI_Send(&file_name_size, 1, MPI_INT, replica_node_ranks[i], UPLOAD_TAG, MPI_COMM_WORLD);
                        MPI_Send(file_name.c_str(), file_name_size, MPI_CHAR, replica_node_ranks[i], UPLOAD_TAG, MPI_COMM_WORLD);
                        int chunk_data_size = chunk_data.size();
                        MPI_Send(&chunk_data_size, 1, MPI_INT, replica_node_ranks[i], UPLOAD_TAG, MPI_COMM_WORLD);
                        MPI_Send(chunk_data.c_str(), chunk_data_size, MPI_CHAR, replica_node_ranks[i], UPLOAD_TAG, MPI_COMM_WORLD);
                        increment_size(chunk_size_set, replica_node_ranks[i]);
                    }
                    ChunkMetaData chunk_metadata;
                    chunk_metadata.chunk_id = chunk_id;
                    replica_node_ranks.erase(remove(replica_node_ranks.begin(), replica_node_ranks.end(), -1), replica_node_ranks.end());
                    chunk_metadata.replica_node_ranks = replica_node_ranks;
                    files[file_name].chunks.push_back(chunk_metadata);
                    files[file_name].offsets.push_back(offset);
                    offset += file.gcount();
                    chunk_id++;
                }
                cout << 1 << endl;
                for (int i = 0; i < files[file_name].chunks.size(); i++)
                {
                    cout << files[file_name].chunks[i].chunk_id << " ";
                    int valid_count = 0;
                    for (int node : files[file_name].chunks[i].replica_node_ranks)
                    {
                        if (failed_nodes.find(node) == failed_nodes.end())
                        {
                            valid_count++;
                        }
                    }
                    cout << valid_count << " ";
                    for (int node : files[file_name].chunks[i].replica_node_ranks)
                    {
                        if (failed_nodes.find(node) == failed_nodes.end())
                        {
                            cout << node << " ";
                        }
                    }
                    cout << endl;
                }
            }
            else if (command == "list_file")
            {
                string file_name;
                ss >> file_name;
                if (files.find(file_name) == files.end())
                {
                    cout << -1 << endl;
                    continue;
                }
                for (int i = 0; i < files[file_name].chunks.size(); i++)
                {
                    cout << files[file_name].chunks[i].chunk_id << " ";
                    int valid_count = 0;
                    for (int node : files[file_name].chunks[i].replica_node_ranks)
                    {
                        if (failed_nodes.find(node) == failed_nodes.end())
                        {
                            valid_count++;
                        }
                    }
                    cout << valid_count << " ";
                    for (int node : files[file_name].chunks[i].replica_node_ranks)
                    {
                        if (failed_nodes.find(node) == failed_nodes.end())
                        {
                            cout << node << " ";
                        }
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
                bool flag = true;
                for (int i = 0; i < file_metadata.chunks.size(); i++)
                {
                    ChunkMetaData chunk_metadata = file_metadata.chunks[i];
                    Body body;
                    body.request_type = RETRIEVE_TAG;
                    body.chunk_id = chunk_metadata.chunk_id;
                    int rank_to_send = -1;
                    for (int j = 0; j < chunk_metadata.replica_node_ranks.size(); j++)
                    {
                        if (failed_nodes.find(chunk_metadata.replica_node_ranks[j]) == failed_nodes.end())
                        {
                            rank_to_send = chunk_metadata.replica_node_ranks[j];
                            break;
                        }
                    }
                    if (rank_to_send == -1)
                    {
                        flag = false;
                        break;
                    }
                }
                if (!flag)
                {
                    cout << -1 << endl;
                }
                else
                {
                    for (int i = 0; i < file_metadata.chunks.size(); i++)
                    {
                        ChunkMetaData chunk_metadata = file_metadata.chunks[i];
                        Body body;
                        body.request_type = RETRIEVE_TAG;
                        body.chunk_id = chunk_metadata.chunk_id;
                        int rank_to_send = -1;
                        for (int j = 0; j < chunk_metadata.replica_node_ranks.size(); j++)
                        {
                            if (failed_nodes.find(chunk_metadata.replica_node_ranks[j]) == failed_nodes.end())
                            {
                                rank_to_send = chunk_metadata.replica_node_ranks[j];
                                break;
                            }
                        }
                        MPI_Send(&body, 1, MPI_BODY, rank_to_send, RETRIEVE_TAG, MPI_COMM_WORLD);
                        int file_name_size = file_name.size();
                        MPI_Send(&file_name_size, 1, MPI_INT, rank_to_send, RETRIEVE_TAG, MPI_COMM_WORLD);
                        MPI_Send(file_name.c_str(), file_name_size, MPI_CHAR, rank_to_send, RETRIEVE_TAG, MPI_COMM_WORLD);
                    }
                    for (int i = 0; i < file_metadata.chunks.size(); i++)
                    {
                        int chunk_size;
                        MPI_Status status;
                        int rank_to_receive_from = -1;
                        for (int j = 0; j < file_metadata.chunks[i].replica_node_ranks.size(); j++)
                        {
                            if (failed_nodes.find(file_metadata.chunks[i].replica_node_ranks[j]) == failed_nodes.end())
                            {
                                rank_to_receive_from = file_metadata.chunks[i].replica_node_ranks[j];
                                break;
                            }
                        }
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
            }
            else if (command == "search")
            {
                string file_name;
                ss >> file_name;
                string word;
                ss >> word;
                if (files.find(file_name) == files.end())
                {
                    cout << -1 << endl;
                    continue;
                }
                set<int> nodes_with_chunks;
                bool chunk_not_found = false;
                for (int i = 0; i < files[file_name].chunks.size(); i++)
                {
                    bool flag = false;
                    for (int j = 0; j < files[file_name].chunks[i].replica_node_ranks.size(); j++)
                    {
                        if (failed_nodes.find(files[file_name].chunks[i].replica_node_ranks[j]) == failed_nodes.end())
                        {
                            nodes_with_chunks.insert(files[file_name].chunks[i].replica_node_ranks[j]);
                            flag = true;
                        }
                    }
                    if (!flag)
                    {
                        chunk_not_found = true;
                        break;
                    }
                }
                if (chunk_not_found)
                {
                    cout << -1 << endl;
                    continue;
                }
                Body body;
                body.request_type = SEARCH_TAG;
                for (int node_rank : nodes_with_chunks)
                {
                    MPI_Send(&body, 1, MPI_BODY, node_rank, SEARCH_TAG, MPI_COMM_WORLD);
                    int file_name_size = file_name.size();
                    MPI_Send(&file_name_size, 1, MPI_INT, node_rank, SEARCH_TAG, MPI_COMM_WORLD);
                    MPI_Send(file_name.c_str(), file_name_size, MPI_CHAR, node_rank, SEARCH_TAG, MPI_COMM_WORLD);
                    int word_size = word.size();
                    MPI_Send(&word_size, 1, MPI_INT, node_rank, SEARCH_TAG, MPI_COMM_WORLD);
                    MPI_Send(word.c_str(), word_size, MPI_CHAR, node_rank, SEARCH_TAG, MPI_COMM_WORLD);
                }
                vector<pair<int, string>> received_chunks;
                for (int node_rank : nodes_with_chunks)
                {
                    int num_chunks;
                    MPI_Status status;
                    MPI_Recv(&num_chunks, 1, MPI_INT, node_rank, SEARCH_TAG, MPI_COMM_WORLD, &status);
                    for (int i = 0; i < num_chunks; i++)
                    {
                        int chunk_id;
                        MPI_Recv(&chunk_id, 1, MPI_INT, node_rank, SEARCH_TAG, MPI_COMM_WORLD, &status);
                        int chunk_size;
                        MPI_Recv(&chunk_size, 1, MPI_INT, node_rank, SEARCH_TAG, MPI_COMM_WORLD, &status);
                        string chunk_data;
                        chunk_data.resize(chunk_size);
                        MPI_Recv(&chunk_data[0], chunk_size, MPI_CHAR, node_rank, SEARCH_TAG, MPI_COMM_WORLD, &status);
                        received_chunks.emplace_back(chunk_id, chunk_data);
                    }
                }
                sort(received_chunks.begin(), received_chunks.end());
                received_chunks.erase(unique(received_chunks.begin(), received_chunks.end()), received_chunks.end());
                vector<int> keyword_offsets;
                for (int i = 0; i < received_chunks.size(); i++)
                {
                    int id_chunk = received_chunks[i].first;
                    int offset = files[file_name].offsets[id_chunk];
                    string current_chunk_string = received_chunks[i].second;
                    string next_chunk_string;
                    if (i + 1 < received_chunks.size() && received_chunks[i + 1].first == id_chunk + 1)
                    {
                        next_chunk_string = received_chunks[i + 1].second;
                    }
                    string combined_string = current_chunk_string + next_chunk_string;
                    size_t start = 0, end;
                    while ((end = combined_string.find(' ', start)) != string::npos)
                    {
                        string extracted_word = combined_string.substr(start, end - start);
                        if (extracted_word == word)
                        {
                            if (start < current_chunk_string.size())
                            {
                                keyword_offsets.push_back(offset + start);
                            }
                        }
                        start = end + 1;
                        if (start >= current_chunk_string.size())
                        {
                            break;
                        }
                    }
                    if (start < current_chunk_string.size())
                    {
                        string last_word = combined_string.substr(start);
                        if (last_word == word)
                        {
                            keyword_offsets.push_back(offset + start);
                        }
                    }
                }
                cout << keyword_offsets.size() << endl;
                for (int offset : keyword_offsets)
                {
                    cout << offset << " ";
                }
                cout << endl;
            }
            else if (command == "failover")
            {
                int failover_rank;
                ss >> failover_rank;
                failed_nodes.insert(failover_rank);
                cout << 1 << endl;
                auto it = std::find_if(chunk_size_set.begin(), chunk_size_set.end(), [failover_rank](const std::pair<int, int> &p)
                                       { return p.second == failover_rank; });
                if (it != chunk_size_set.end())
                {
                    std::pair<int, int> updated = *it;
                    chunk_size_set.erase(it);
                    updated.first = 0;
                    chunk_size_set.insert(updated);
                }
                Body body = {FAILOVER_TAG, 0, rank, true};
                MPI_Send(&body, 1, MPI_BODY, failover_rank, FAILOVER_TAG, MPI_COMM_WORLD);
            }
            else if (command == "recover")
            {
                int recover_rank;
                ss >> recover_rank;
                Body body = {RECOVER_TAG, 0, rank, false};
                MPI_Send(&body, 1, MPI_BODY, recover_rank, RECOVER_TAG, MPI_COMM_WORLD);
                failed_nodes.erase(recover_rank);
                cout << 1 << endl;
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
                string file_name;
                int file_name_size;
                MPI_Recv(&file_name_size, 1, MPI_INT, 0, SEARCH_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                file_name.resize(file_name_size);
                MPI_Recv(&file_name[0], file_name_size, MPI_CHAR, 0, SEARCH_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                string word;
                int word_size;
                MPI_Recv(&word_size, 1, MPI_INT, 0, SEARCH_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                word.resize(word_size);
                MPI_Recv(&word[0], word_size, MPI_CHAR, 0, SEARCH_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                vector<pair<int, string>> chunks;
                for (int i = 0; i < storage[file_name].size(); i++)
                {
                    string chunk_string = storage[file_name][i].data;
                    stringstream ss(chunk_string);
                    string first_word, last_word;
                    string temp;
                    bool flag = false;
                    while (ss >> temp)
                    {
                        if (first_word.empty())
                        {
                            first_word = temp;
                        }
                        last_word = temp;
                        if (temp == word)
                        {
                            flag = true;
                            break;
                        }
                    }
                    if (!flag && (chunk_string.find(first_word) != string::npos || chunk_string.find(last_word) != string::npos))
                    {
                        flag = true;
                    }
                    if (flag)
                    {
                        chunks.emplace_back(storage[file_name][i].chunk_id, storage[file_name][i].data);
                    }
                }
                int num_chunks = chunks.size();
                MPI_Send(&num_chunks, 1, MPI_INT, 0, SEARCH_TAG, MPI_COMM_WORLD);
                for (int i = 0; i < num_chunks; i++)
                {
                    int chunk_id = chunks[i].first;
                    int chunk_data_size = chunks[i].second.size();
                    MPI_Send(&chunk_id, 1, MPI_INT, 0, SEARCH_TAG, MPI_COMM_WORLD);
                    MPI_Send(&chunk_data_size, 1, MPI_INT, 0, SEARCH_TAG, MPI_COMM_WORLD);
                    MPI_Send(chunks[i].second.c_str(), chunk_data_size, MPI_CHAR, 0, SEARCH_TAG, MPI_COMM_WORLD);
                }
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