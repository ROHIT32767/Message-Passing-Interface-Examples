#include <stddef.h>
#include <iostream>
#include <algorithm>
#include "mpi.h"
#include <set>
#include <map>
#include <bits/stdc++.h>

using namespace std;
typedef long long ll;

struct ball
{
    int ball_index;
    int x;
    int y;
    char ball_direction;
    bool operator==(const ball &temp) const
    {
        return (x == temp.x && y == temp.y and ball_direction == temp.ball_direction);
    }
};

bool compareParticles(const ball &ball1, const ball &ball2)
{
    return ball2.ball_index > ball1.ball_index;
}

bool shouldRemoveParticle(const ball &ball_object)
{
    if(ball_object.x == -1 && ball_object.y == -1)
    {
        return true;
    }
    return false;
}

pair<ll, ll> get_row_range(ll process_number, ll total_processes, ll total_rows)
{
    ll div = total_rows / total_processes;
    ll rem = total_rows % total_processes;
    ll start_vertex, end_vertex;
    if (process_number < rem)
    {
        start_vertex = process_number * (div + 1);
        end_vertex = start_vertex + div;
    }
    else
    {
        start_vertex = rem * (div + 1) + (process_number - rem) * div;
        end_vertex = start_vertex + div - 1;
    }
    if (start_vertex > end_vertex)
    {
        return make_pair(1000, -1000);
    }
    return make_pair(start_vertex, end_vertex);
}

ll get_process_number(ll row, ll total_processes, ll total_rows)
{
    ll div = total_rows / total_processes;
    ll rem = total_rows % total_processes;
    if (row < rem * (div + 1))
    {
        return row / (div + 1);
    }
    else
    {
        return rem + (row - rem * (div + 1)) / div;
    }
}

MPI_Datatype MPI_BALL;

char nextDirection(char ball_direction)
{
    if (ball_direction == 'D')
        return 'L';
    else if (ball_direction == 'L')
        return 'U';
    else if (ball_direction == 'U')
        return 'R';
    else if (ball_direction == 'R')
        return 'D';
    return ball_direction;
}

char opposite_direction(char ball_direction)
{
    if (ball_direction == 'D')
        return 'U';
    else if (ball_direction == 'L')
        return 'R';
    else if (ball_direction == 'U')
        return 'D';
    else if (ball_direction == 'R')
        return 'L';
    return ball_direction;
}

void handle_collision(vector<ball> &balls)
{
    map<ll, ll> map_index;
    for (ll i = 0; i+1 <= balls.size(); i++)
    {
        map_index[balls[i].ball_index] = i;
    }
    map< pair<ll, ll>, set<ll> > map_position;
    for (ll i = 0; i+1 <= balls.size(); i++)
    {
        map_position[make_pair(balls[i].x, balls[i].y)].insert(balls[i].ball_index);
    }

    for (auto &entry : map_position)
    {
        auto &position = entry.first;
        auto &particle_indices = entry.second;
        ll count = particle_indices.size();
        if (count == 2)
        {
            ll first_ball = -1;
            ll second_ball = -1;
            auto iterator_set = particle_indices.begin();
            ll temp1 = *iterator_set;
            iterator_set++;
            ll temp2 = *iterator_set;
            first_ball = map_index[temp1];
            second_ball = map_index[temp2];
            if (first_ball != -1 && second_ball != -1)
            {
                balls[first_ball].ball_direction = nextDirection(balls[first_ball].ball_direction);
                balls[second_ball].ball_direction = nextDirection(balls[second_ball].ball_direction);
            }
        }
        else if (count == 4)
        {
            ll first_ball = -1;
            ll second_ball = -1;
            ll third_ball = -1;
            ll fourth_ball = -1;
            auto iterator_set = particle_indices.begin();
            ll temp1 = *iterator_set;
            iterator_set++;
            ll temp2 = *iterator_set;
            iterator_set++;
            ll temp3 = *iterator_set;
            iterator_set++;
            ll temp4 = *iterator_set;
            first_ball = map_index[temp1];
            second_ball = map_index[temp2];
            third_ball = map_index[temp3];
            fourth_ball = map_index[temp4];
            if (first_ball != -1 && second_ball != -1 && third_ball != -1 && fourth_ball != -1)
            {
                balls[first_ball].ball_direction = opposite_direction(balls[first_ball].ball_direction);
                balls[second_ball].ball_direction = opposite_direction(balls[second_ball].ball_direction);
                balls[third_ball].ball_direction = opposite_direction(balls[third_ball].ball_direction);
                balls[fourth_ball].ball_direction = opposite_direction(balls[fourth_ball].ball_direction);
            }
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
}

int main(int argc, char *argv[])
{
    ios_base::sync_with_stdio(false);
    cin.tie(NULL);
    MPI_Init(&argc, &argv);
    int rank, size;
    string input_file = argv[1];
    string output_file = argv[2];
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int N, M, K, T;
    if (rank == 0)
    {
        freopen(input_file.c_str(), "r", stdin);
        cin >> N >> M >> K >> T;
    }
    MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&M, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&K, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&T, 1, MPI_INT, 0, MPI_COMM_WORLD);
    int blocklength = sizeof(ball);
    MPI_Type_contiguous(blocklength, MPI_BYTE, &MPI_BALL);
    MPI_Type_commit(&MPI_BALL);
    vector<ball> balls(K);
    vector<int> ball_count(size, 0);
    if (rank == 0)
    {
        for (ll i = 0; i <= K-1; i++)
        {
            cin >> balls[i].x >> balls[i].y >> balls[i].ball_direction;
            balls[i].ball_index = i;
        }
        for (auto &ball_temp : balls)
        {
            ll process = get_process_number(ball_temp.y, size, M);
            MPI_Send(&ball_temp, 1, MPI_BALL, process, 0, MPI_COMM_WORLD);
            ball_count[process]++;
        }
        fclose(stdin);
    }
    MPI_Bcast(&ball_count[0], size, MPI_INT, 0, MPI_COMM_WORLD);
    ll num_balls = ball_count[rank];
    balls.resize(num_balls);
    for (ll i = 0; i <= num_balls-1; i++)
    {
        MPI_Recv(&balls[i], 1, MPI_BALL, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    pair<ll, ll> row_range = get_row_range(rank, size, M);
    ll start_row = row_range.first;
    ll end_row = row_range.second;
    ll root = 0;
    MPI_Barrier(MPI_COMM_WORLD);
    for (ll i = 0; i <= T-1; i++)
    {
        for (auto &ball_temp : balls)
        {
            if (ball_temp.x == 0 && ball_temp.ball_direction == 'U')
            {
                ball_temp.x = N;
            }
            else if (ball_temp.x == N - 1 && ball_temp.ball_direction == 'D')
            {
                ball_temp.x = -1;
            }
            else if (ball_temp.y == 0 && ball_temp.ball_direction == 'L')
            {
                ball_temp.y = M;
            }
            else if (ball_temp.y == M - 1 && ball_temp.ball_direction == 'R')
            {
                ball_temp.y = -1;
            }
        }
        MPI_Barrier(MPI_COMM_WORLD);
        for (auto &ball_temp : balls)
        {
            MPI_Request request;
            bool send_flag = false;
            if (ball_temp.y == -1 && ball_temp.ball_direction == 'R')
            {
                ll proc_num = get_process_number(0, size, M);
                MPI_Send(&ball_temp, 1, MPI_BALL, proc_num, 0, MPI_COMM_WORLD);
                send_flag = true;
            }
            else if (ball_temp.y == M && ball_temp.ball_direction == 'L')
            {
                ll proc_num = get_process_number(M - 1, size, M);
                MPI_Send(&ball_temp, 1, MPI_BALL, proc_num, 0, MPI_COMM_WORLD);
                send_flag = true;
            }
            else if (ball_temp.y == start_row && (rank - 1) >= 0 && ball_temp.ball_direction == 'L')
            {
                MPI_Send(&ball_temp, 1, MPI_BALL, rank - 1, 0, MPI_COMM_WORLD);
                send_flag = true;
            }
            else if (ball_temp.y == end_row && (rank + 1) < size && ball_temp.ball_direction == 'R')
            {
                MPI_Send(&ball_temp, 1, MPI_BALL, rank + 1, 0, MPI_COMM_WORLD);
                send_flag = true;
            }

            if(send_flag){
                ball_temp.x = -1;
                ball_temp.y = -1;
            }
        }
        balls.erase(std::remove_if(balls.begin(), balls.end(), shouldRemoveParticle), balls.end());
        MPI_Barrier(MPI_COMM_WORLD);
        if (rank <= size-2)
        {
            MPI_Status status;
            MPI_Request request;
            int flag = 0;
            MPI_Iprobe(rank + 1, 0, MPI_COMM_WORLD, &flag, &status);
            while (flag)
            {
                ball send_object;
                MPI_Recv(&send_object, 1, MPI_BALL, rank + 1, 0, MPI_COMM_WORLD, &status);
                balls.push_back(send_object);
                MPI_Iprobe(rank + 1, 0, MPI_COMM_WORLD, &flag, &status);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);

        if (rank >= 1)
        {
            MPI_Status status;
            MPI_Request request;
            int flag = 0;
            MPI_Iprobe(rank - 1, 0, MPI_COMM_WORLD, &flag, &status);
            while (flag)
            {
                ball send_object;
                MPI_Recv(&send_object, 1, MPI_BALL, rank - 1, 0, MPI_COMM_WORLD, &status);
                balls.push_back(send_object);
                MPI_Iprobe(rank - 1, 0, MPI_COMM_WORLD, &flag, &status);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);

        if (rank == 0)
        {
            MPI_Status status;
            MPI_Request request;
            int flag = 0;
            ll last_process_allocated = get_process_number(M - 1, size, M);
            MPI_Iprobe(last_process_allocated, 0, MPI_COMM_WORLD, &flag, &status);
            while (flag)
            {
                ball send_object;
                MPI_Recv(&send_object, 1, MPI_BALL, last_process_allocated, 0, MPI_COMM_WORLD, &status);
                balls.push_back(send_object);
                MPI_Iprobe(last_process_allocated, 0, MPI_COMM_WORLD, &flag, &status);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);

        ll last_process_allocated_rank = get_process_number(M - 1, size, M);
        if (rank == last_process_allocated_rank)
        {
            MPI_Status status;
            MPI_Request request;
            int flag = 0;
            ll first_process_allocated = get_process_number(0, size, M);
            MPI_Iprobe(first_process_allocated, 0, MPI_COMM_WORLD, &flag, &status);
            while (flag)
            {
                ball send_object;
                MPI_Recv(&send_object, 1, MPI_BALL, first_process_allocated, 0, MPI_COMM_WORLD, &status);
                balls.push_back(send_object);
                MPI_Iprobe(first_process_allocated, 0, MPI_COMM_WORLD, &flag, &status);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);
        for (auto &ball_temp : balls)
        {
            ll x = ball_temp.x;
            ll y = ball_temp.y;
            if (ball_temp.ball_direction == 'L')
            {
                y = y - 1;
            }
            else if (ball_temp.ball_direction == 'R')
            {
                y = y + 1;
            }
            else if (ball_temp.ball_direction == 'U')
            {
                x = x - 1;
            }
            else if (ball_temp.ball_direction == 'D')
            {
                x = x + 1;
            }
            ball_temp.x = x;
            ball_temp.y = y;
        }
        MPI_Barrier(MPI_COMM_WORLD);
        handle_collision(balls);
        MPI_Barrier(MPI_COMM_WORLD);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    vector<ball> collected_balls;
    ll total_balls = 0;
    ll particle_size = balls.size();
    vector<int> count_received(size);
    vector<int> displacements(size);
    MPI_Gather(&particle_size, 1, MPI_INT, &count_received[0], 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (rank == 0)
    {
        total_balls = accumulate(count_received.begin(), count_received.end(), 0);
        collected_balls.resize(total_balls);
        displacements[0] = 0;
        for (int i = 0; i < size-1; i++)
        {
            displacements[i+1] = displacements[i];
            displacements[i+1] += count_received[i];
        }
    }
    MPI_Gatherv(&balls[0], balls.size(), MPI_BALL, &collected_balls[0], &count_received[0], &displacements[0], MPI_BALL, 0, MPI_COMM_WORLD);
    if (rank == 0)
    {
        freopen(output_file.c_str(), "w", stdout);
        sort(collected_balls.begin(), collected_balls.end(), compareParticles);
        for (auto &ball_temp : collected_balls)
        {
            cout << ball_temp.x << " " << ball_temp.y << " " << ball_temp.ball_direction << endl;
        }
        fclose(stdout);
    }
    MPI_Finalize();
    return 0;
}