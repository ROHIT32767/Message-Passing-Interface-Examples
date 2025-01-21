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
#include <set>
#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>
#define CHUNK_SIZE 32
#define REPLICATION_FACTOR 3
#define UPLOAD_TAG 1
#define RETRIEVE_TAG 2
#define SEARCH_TAG 3
#define FAILOVER_TAG 4
#define RECOVER_TAG 5
#define EXIT_TAG 6
#define HEARTBEAT_TAG 7

#define HEARTBEAT_INTERVAL 1000  
#define FAILOVER_INTERVAL 3000  

using namespace std;
using namespace std::chrono_literals;
mutex metadata_mutex;
unordered_map<int, chrono::steady_clock::time_point> last_heartbeat; 
unordered_map<int, bool> node_status; 
unordered_map<string, vector<int>> chunk_metadata; 
mutex cout_mutex;

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

void increment_size(std::multiset<std::pair<int, int>, Compare> &chunk_size_set, int node){
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

vector<int> get_replicate_node_ranks(multiset<pair<int, int>, Compare> &chunk_size_set, int N,set<int> &failed_nodes){
    // get the first 3 nodes with the least number of chunks that are not failed
    vector<int> replica_nodes;
    auto it = chunk_size_set.begin();
    for (int i = 0; i < 3; ++i)
    {
        while(it != chunk_size_set.end() && failed_nodes.find(it->second) != failed_nodes.end()){
            it++;
        }
        if(it != chunk_size_set.end()){
            replica_nodes.push_back(it->second);
            it++;
        }else{
            replica_nodes.push_back(-1);
        }
    }
    return replica_nodes;
}

void heartbeat_sender(int rank, int root_rank, condition_variable& cv, bool& is_active, mutex& active_mutex, MPI_Comm comm, bool& should_stop) {
    Body signal;
    signal.sender_rank = rank;
    signal.stop = false;    
    while (!should_stop) {
        {
            unique_lock<mutex> lock(active_mutex);
            if (!is_active) {
                cv.wait(lock); // Wait for recovery signal
                cout_mutex.lock();
                cout << "Node " << rank << " recovered. Resuming heartbeats in heartbeat_sender." << endl;
                cout_mutex.unlock();
            }
        }
        MPI_Send(&signal, 1, MPI_BODY, root_rank, HEARTBEAT_TAG, comm);
        this_thread::sleep_for(chrono::milliseconds(HEARTBEAT_INTERVAL));
    }
}

// Root node's function to monitor heartbeats
void heartbeat_monitor(int rank, MPI_Comm comm, bool& should_stop) {
    Body signal;
    while (!should_stop) {
        MPI_Recv(&signal, 1, MPI_BODY, MPI_ANY_SOURCE, HEARTBEAT_TAG, comm, MPI_STATUS_IGNORE);
        if (signal.stop) {
            lock_guard<mutex> lock(metadata_mutex);
            node_status[signal.sender_rank] = false;
            cout_mutex.lock();
            cout << "Node " << signal.sender_rank << " marked as down." << endl;
            cout_mutex.unlock();
            continue;
        }
        lock_guard<mutex> lock(metadata_mutex);
        last_heartbeat[signal.sender_rank] = chrono::steady_clock::now();
        if(!node_status[signal.sender_rank]){
            cout_mutex.lock();
            cout << "Node " << signal.sender_rank << " recovered in heartbeat_monitor." << endl;
            cout_mutex.unlock();
        }
        node_status[signal.sender_rank] = true; // Mark as active
    }
}

// Periodic check for failed nodes
void failover_checker(bool& should_stop) {
    while (!should_stop) {
        this_thread::sleep_for(chrono::seconds(1));
        auto now = chrono::steady_clock::now();
        lock_guard<mutex> lock(metadata_mutex);
        for (auto& [rank, timestamp] : last_heartbeat) {
            if (node_status[rank] && chrono::duration_cast<chrono::milliseconds>(now - timestamp).count() > FAILOVER_INTERVAL) {
                node_status[rank] = false;
                cout_mutex.lock();
                cout << "Failover: Node " << rank << " is down (no heartbeat)." << endl;
                cout_mutex.unlock();
            }
        }
    }
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
        // Master Metadata Server
        map<string, FileMetaData> files;
        multiset<pair<int, int>, Compare> chunk_size_set;
        for (int i = 1; i < size; i++)
        {
            chunk_size_set.insert({0, i});
        }
        int N = size;
        string command;
        set<int> failed_nodes;
        std::filesystem::path cpp_file_path(__FILE__);
        std::filesystem::path cpp_directory = cpp_file_path.parent_path();
        bool should_stop = false;
        thread monitor_thread(heartbeat_monitor, rank, MPI_COMM_WORLD, ref(should_stop));
        thread checker_thread(failover_checker, ref(should_stop));
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
                std::filesystem::path full_path = cpp_directory / relative_path;
                ifstream file(full_path);
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
                    // remove -1 from replica_node_ranks
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
                for (int i = 0; i < files[file_name].chunks.size(); i++)
                {
                    for (int j = 0; j < files[file_name].chunks[i].replica_node_ranks.size(); j++)
                    {
                        nodes_with_chunks.insert(files[file_name].chunks[i].replica_node_ranks[j]);
                    }
                }
                Body body;
                body.request_type = SEARCH_TAG;
                for (int node_rank : nodes_with_chunks)
                {
                    // send request type
                    MPI_Send(&body, 1, MPI_BODY, node_rank, SEARCH_TAG, MPI_COMM_WORLD);
                    int file_name_size = file_name.size();
                    // send file name size followed by filename
                    MPI_Send(&file_name_size, 1, MPI_INT, node_rank, SEARCH_TAG, MPI_COMM_WORLD);
                    MPI_Send(file_name.c_str(), file_name_size, MPI_CHAR, node_rank, SEARCH_TAG, MPI_COMM_WORLD);
                    int word_size = word.size();
                    // send word size followed by word
                    MPI_Send(&word_size, 1, MPI_INT, node_rank, SEARCH_TAG, MPI_COMM_WORLD);
                    MPI_Send(word.c_str(), word_size, MPI_CHAR, node_rank, SEARCH_TAG, MPI_COMM_WORLD);
                }
                vector<pair<int, string>> received_chunks;
                for (int node_rank : nodes_with_chunks)
                {
                    int num_chunks;
                    MPI_Status status;
                    // get number of relevant chunks
                    MPI_Recv(&num_chunks, 1, MPI_INT, node_rank, SEARCH_TAG, MPI_COMM_WORLD, &status);
                    for (int i = 0; i < num_chunks; i++)
                    {
                        int chunk_id;
                        // send chunk_id
                        MPI_Recv(&chunk_id, 1, MPI_INT, node_rank, SEARCH_TAG, MPI_COMM_WORLD, &status);
                        int chunk_size;
                        // send chunk_size
                        MPI_Recv(&chunk_size, 1, MPI_INT, node_rank, SEARCH_TAG, MPI_COMM_WORLD, &status);
                        string chunk_data;
                        chunk_data.resize(chunk_size);
                        // send chunk
                        MPI_Recv(&chunk_data[0], chunk_size, MPI_CHAR, node_rank, SEARCH_TAG, MPI_COMM_WORLD, &status);
                        received_chunks.emplace_back(chunk_id, chunk_data);
                    }
                }
                // remove duplicates
                sort(received_chunks.begin(), received_chunks.end());
                received_chunks.erase(unique(received_chunks.begin(), received_chunks.end()), received_chunks.end());
                vector<int> keyword_offsets;
                for (int i = 0; i < received_chunks.size(); i++)
                {
                    int id_chunk = received_chunks[i].first;
                    int offset = files[file_name].offsets[id_chunk];
                    size_t start = 0;
                    size_t end;
                    string last_word;
                    string next_word;
                    while ((end = received_chunks[i].second.find(' ', start)) != string::npos)
                    {
                        if (received_chunks[i].second.substr(start, end - start) == word)
                        {
                            keyword_offsets.push_back(offset + start);
                        }
                        last_word = received_chunks[i].second.substr(start, end - start);
                        start = end + 1;
                    }
                    if (i + 1 < received_chunks.size())
                    {
                        while ((end = received_chunks[i + 1].second.find(word, start)) != string::npos)
                        {
                            next_word = received_chunks[i + 1].second.substr(start, end - start);
                            break;
                        }
                        if (last_word + next_word == word)
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
                Body signal;
                signal.stop = true;
                MPI_Send(&signal, 1, MPI_BODY, failover_rank, FAILOVER_TAG, MPI_COMM_WORLD);
                lock_guard<mutex> lock(metadata_mutex);
                node_status[failover_rank] = false;
                cout_mutex.lock();
                cout << "Failover triggered for rank " << failover_rank << "." << endl;
                cout_mutex.unlock();
            }
            else if (command == "recover")
            {
                int recover_rank;
                ss >> recover_rank;
                failed_nodes.erase(recover_rank);
                cout << 1 << endl;
                Body signal;
                signal.stop = false;
                MPI_Send(&signal, 1, MPI_BODY, recover_rank, RECOVER_TAG, MPI_COMM_WORLD);
                cout_mutex.lock();
                cout << "Recovery triggered for rank " << recover_rank << "." << endl;
                cout_mutex.unlock();
            }
            else if (command == "exit")
            {
                for (int i = 1; i < size; i++)
                {
                    Body body;
                    MPI_Send(&body, 1, MPI_BODY, i, EXIT_TAG, MPI_COMM_WORLD);
                }
                should_stop = true;
                break;
            }
        }
        monitor_thread.join();
        checker_thread.join();
        cout << "running " << rank << endl;
    }
    else
    {
        map<string, vector<Chunk>> storage;
        condition_variable cv;
        mutex active_mutex;
        bool is_active = true;
        bool should_stop = false; 
        thread sender_thread(heartbeat_sender, rank, 0, ref(cv), ref(is_active), ref(active_mutex), MPI_COMM_WORLD, ref(should_stop));
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
                // recieve file name size
                MPI_Recv(&file_name_size, 1, MPI_INT, 0, SEARCH_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                file_name.resize(file_name_size);
                // recieve filename
                MPI_Recv(&file_name[0], file_name_size, MPI_CHAR, 0, SEARCH_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                string word;
                int word_size;
                // recieve word size
                MPI_Recv(&word_size, 1, MPI_INT, 0, SEARCH_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                word.resize(word_size);
                // recieve word
                MPI_Recv(&word[0], word_size, MPI_CHAR, 0, SEARCH_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                vector<pair<int, string>> chunks;
                for (int i = 0; i < storage[file_name].size(); i++)
                {
                    if (storage[file_name][i].data.find(word) != string::npos)
                    {
                        chunks.emplace_back(storage[file_name][i].chunk_id, storage[file_name][i].data);
                    }
                }
                int num_chunks = chunks.size();
                // send number of relevant chunks
                MPI_Send(&num_chunks, 1, MPI_INT, 0, SEARCH_TAG, MPI_COMM_WORLD);
                for (int i = 0; i < num_chunks; i++)
                {
                    int chunk_id = chunks[i].first;
                    int chunk_data_size = chunks[i].second.size();
                    // send chunk_id
                    MPI_Send(&chunk_id, 1, MPI_INT, 0, SEARCH_TAG, MPI_COMM_WORLD);
                    // send chunk size
                    MPI_Send(&chunk_data_size, 1, MPI_INT, 0, SEARCH_TAG, MPI_COMM_WORLD);
                    // send chunk
                    MPI_Send(chunks[i].second.c_str(), chunk_data_size, MPI_CHAR, 0, SEARCH_TAG, MPI_COMM_WORLD);
                }
            }
            else if (status.MPI_TAG == FAILOVER_TAG)
            {
                lock_guard<mutex> lock(active_mutex);
                is_active = false;
                cout_mutex.lock();
                cout << "Node " << rank << " received failover signal. Stopping heartbeats." << endl;
                cout_mutex.unlock();

                Body ack_signal;
                ack_signal.sender_rank = rank;
                ack_signal.stop = true; // Indicate this node has stopped
                MPI_Send(&ack_signal, 1, MPI_BODY, 0, HEARTBEAT_TAG, MPI_COMM_WORLD);
            }
            else if (status.MPI_TAG == RECOVER_TAG)
            {
                lock_guard<mutex> lock(active_mutex);
                is_active = true;
                cv.notify_all();
                cout_mutex.lock();
                cout << "Node " << rank << " recovered. Resuming heartbeats." << endl;
                cout_mutex.unlock();
            }
            else if (status.MPI_TAG == EXIT_TAG)
            {
                
                break;
            }
            cout << "running " << rank << endl;
        }
        should_stop = true; 
        sender_thread.join();
    }
    MPI_Finalize();
    return 0;
}