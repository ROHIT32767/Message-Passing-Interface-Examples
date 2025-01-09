
#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <time.h>
#include <limits.h>
#include <bits/stdc++.h>
#include <vector>
#include <map>
#include <algorithm>
#include <set>
using namespace std;
struct Vec
{
    int y;
    int x;
    char d;
};
bool compare_entry( const Vec & e1, const Vec & e2) {
  if( e1.x != e2.x)
    return (e1.x < e2.x);
  return (e1.y < e2.y);
}


int main(int argc, char *argv[])
{

    MPI_Init(&argc, &argv);
    int size, rank, N, M, T;
    srand(time(NULL));
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Barrier(MPI_COMM_WORLD);
    double tbeg = MPI_Wtime();

    if (rank == 0)
    {
        int N, M, K, T;
        scanf("%d %d %d %d", &N, &M, &K, &T);
        MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(&M, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(&T, 1, MPI_INT, 0, MPI_COMM_WORLD);
        
        vector<Vec> v1;
        for (int i = 0; i < K; i++)
        {
            int x, y;
            char d;
            scanf("%d %d %c", &x, &y, &d);
            struct Vec a = {y, x, d};
            v1.push_back(a);
        }
        map<int, int> mp1;
        int nrows = M / size;
        // char arr[nrows][N];
        int len1 = nrows * N;
        int len2 = (M - (size - 1)) * N;
        int row = M - (size - 1) * nrows;
        if(nrows>0)
        {
            for (int i = 0; i < K; i++)
            {
                if ((v1[i].y / nrows) > (size - 1))
                {

                    mp1[size - 1]++;
                }
                else
                {
                    mp1[v1[i].y / nrows]++;
                }
            }
        }
        vector<Vec> v;
        if(nrows>0)
        {
            for (int i = 0; i < K; i++)
            {
                if (v1[i].y / nrows == 0)
                {
                    v.push_back(v1[i]);
                }
            }
        }
        else
        {
            for (int i = 0; i < K; i++)
            {
                v.push_back(v1[i]);
            }
     
        }
        

        if (size != 1 && nrows > 0)
        {
            for (int i = 1; i < size; i++)
            {
                int ss = mp1[i];
                MPI_Send(&ss, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            }
        }
        vector<vector<Vec>>v2(size);

        // printf("before assign\n");
        
        if (size != 1 && nrows > 0)
        {
            for (int i = 0; i < K; i++)
            {
                int r = v1[i].y;

                if ((r / nrows) > (size - 1))
                {
                    struct Vec a;
                    a.x=v1[i].x;
                    a.y=v1[i].y;
                    a.d=v1[i].d;
                    v2[size-1].push_back(a);
                    
                }
                else
                {
                    if (r / nrows != 0)
                    {
                        struct Vec a;
                        a.x=v1[i].x;
                        a.y=v1[i].y;
                        a.d=v1[i].d;
                        v2[r/nrows].push_back(a);
                        
                    }

                    
                }
            }
        }
        // printf("at last\n");
        for(int i=1;i<size;i++)
        {
            MPI_Send(v2[i].data(), mp1[i]* sizeof(Vec), MPI_BYTE, i, 1, MPI_COMM_WORLD);

        }
        // printf(" hhgfff\n");
        int spr=nrows;
        if(nrows==0)
        {
            spr=M;
        }
        int num[spr][N];
        for (int i = 0; i < spr; i++)
        {
            for (int j = 0; j < N; j++)
            {
                num[i][j] = 0;
            }
        }
        for (int i = 0; i < v.size(); i++)
        {
            num[v[i].y][v[i].x]++;
        }
        for (int i = 0; i < T; i++)
        {
            if (size != 1 && nrows > 0)
            {
                int sending = 0;
                int rec = 0;
                vector<Vec> send;
                
                for (int o = 0; o < v.size(); o++)
                {
                    if (v[o].y >= nrows * (rank + 1))
                    {
                        sending++;
                        send.push_back(v[o]);
                        num[v[o].y][v[o].x]--;
                        v.erase(v.begin() + o);
                    }
                }
                MPI_Send(&sending, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD);
                // printf("sending %d \n",sending);
                if(sending>0)
                {
                    MPI_Send(send.data(), sending* sizeof(Vec), MPI_BYTE, rank+1, 1, MPI_COMM_WORLD);

                }
                MPI_Recv(&rec, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                vector<Vec> recv(rec);
                if(rec>0)
                {
                    MPI_Recv(recv.data(), rec*sizeof(Vec), MPI_BYTE, rank+1, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                }
                for (int f = 0; f < rec; f++)
                {
                    
                    v.push_back(recv[f]);
                    num[recv[f].y][recv[f].x]++;
                }
                send.clear();
                recv.clear();

                    

            }
            map<pair<int, int>, vector<char>> mp;
            for (int m = 0; m < v.size(); m++)
            {
                if (num[v[m].y][v[m].x] == 2)
                {
                    mp[{v[m].y, v[m].x}].push_back(v[m].d);
                }
            }

            for (int j = 0; j < v.size(); j++)
            {
                int xaxis = v[j].x;
                int yaxis = v[j].y;
                // if(i==T-1)
                // {
                //      printf("%d %d num  %d %d\n",rank,num[v[j].y-nrows*rank][v[j].x],v[j].x,v[j].y);
                // }

                if (num[v[j].y][v[j].x] == 2 && xaxis != 0 && xaxis != N - 1 && yaxis != 0 && yaxis != M - 1)
                {
                   
                    // printf("jj");
                    // printf("jj ");
                    int cout = 0;
                    // num[v[j].y][v[j].x]--;
                    if (mp[{v[j].y, v[j].x}][0] == 'L' && mp[{v[j].y, v[j].x}][1] == 'R' && cout == 0)
                    {
                        cout = 1;
                        if (v[j].d == 'L')
                        {
                            v[j].d = 'D';
                        }
                        if (v[j].d == 'R')
                        {
                            v[j].d = 'U';
                        }
                    }
                    if (mp[{v[j].y, v[j].x}][0] == 'R' && mp[{v[j].y, v[j].x}][1] == 'L' && cout == 0)
                    {
                        cout = 1;
                        if (v[j].d == 'L')
                        {
                            v[j].d = 'D';
                        }
                        if (v[j].d == 'R')
                        {
                            v[j].d = 'U';
                        }
                    }
                    if (mp[{v[j].y, v[j].x}][0] == 'U' && mp[{v[j].y, v[j].x}][1] == 'D' && cout == 0)
                    {
                        cout = 1;
                        if (v[j].d == 'U')
                        {
                            v[j].d = 'L';
                        }
                        if (v[j].d == 'D')
                        {
                            v[j].d = 'R';
                        }
                    }
                    if (mp[{v[j].y, v[j].x}][0] == 'D' && mp[{v[j].y, v[j].x}][1] == 'U' && cout == 0)
                    {
                        cout = 1;
                        if (v[j].d == 'U')
                        {
                            v[j].d = 'L';
                        }
                        if (v[j].d == 'D')
                        {
                            v[j].d = 'R';
                        }
                    }
                }
            }
            for (int j = 0; j < v.size(); j++)
            {
                int cout = 0;

                if (v[j].d == 'R' && cout == 0)
                {
                    cout = 1;
                    int r = v[j].x + 1;
                    if (r > (N - 1))
                    {
                        num[v[j].y][v[j].x]--;
                        v[j].x--;
                        v[j].d = 'L';
                        // printf("jj");
                        // printf("%c ",v[j].d);
                        num[v[j].y][v[j].x]++;
                    }
                    else
                    {
                        num[v[j].y][v[j].x]--;
                        v[j].x++;
                        num[v[j].y][v[j].x]++;
                    }
                }
                if (v[j].d == 'L' && cout == 0)
                {
                    cout = 1;
                    int l = v[j].x - 1;
                    if (l < 0)
                    {
                        num[v[j].y][v[j].x]--;
                        v[j].x++;
                        v[j].d = 'R';
                        num[v[j].y][v[j].x]++;
                    }
                    else
                    {
                        num[v[j].y][v[j].x]--;
                        v[j].x--;
                        num[v[j].y][v[j].x]++;
                    }
                }
                if (v[j].d == 'U' && cout == 0)
                {
                    cout = 1;
                    int u = v[j].y + 1;
                    if (u > (M - 1))
                    {
                        num[v[j].y][v[j].x]--;
                        v[j].y--;
                        v[j].d = 'D';
                        num[v[j].y][v[j].x]++;
                    }
                    else
                    {
                        num[v[j].y][v[j].x]--;
                        v[j].y++;
                        num[v[j].y][v[j].x]++;
                    }
                }

                if (v[j].d == 'D' && cout == 0)
                {
                    cout = 1;
                    int u = v[j].y - 1;
                    if (u < 0)
                    {
                        num[v[j].y][v[j].x]--;
                        v[j].y++;
                        v[j].d = 'U';
                        num[v[j].y][v[j].x]++;
                    }
                    else
                    {
                        num[v[j].y][v[j].x]--;
                        v[j].y--;
                        num[v[j].y][v[j].x]++;
                    }
                }
            }
            // for(int k=0;k<nrows;k++)
            // {

            //     for(int j=0;j<N;j++)
            //     {
            //         printf("%d ",num[k][j]);
            //     }
            //     printf("\n");
            // }
            // printf("\n");

            mp.clear();
            // for(int p=0;p
            
                
                
            
        }
        // printf(";;;;;;;;;;;;;;;;;;;;;;\n");
        for(int p=1;p<size;p++)
        {
            if(nrows>0)
            {
                int plen;
                MPI_Recv(&plen, 1, MPI_INT, p, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                vector<Vec>final(plen);
                if(plen>0)
                {
                
                    MPI_Recv(final.data(),plen*sizeof(Vec), MPI_BYTE,p, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                }
                for(int q=0;q<plen;q++)
                {
                    v.push_back(final[q]);
                }

            }


        }
        sort( v.begin(), v.end(), compare_entry );

        for (int p = 0; p < v.size(); p++)
        {
            printf("%d %d %c", v[p].x, v[p].y, v[p].d);
            printf("\n");
        }


    }
    else
    {
        MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(&M, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(&T, 1, MPI_INT, 0, MPI_COMM_WORLD);
        int length2;
        int nrows;
        int nr = M / size;
        if (nr > 0)
        {
            if ((size - 1) == rank)
            {
                nrows = M - (size - 1) * (M / size);
            }
            else
            {
                nrows = M / size;
            }
            MPI_Recv(&length2, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            vector<Vec> v(length2);
            MPI_Recv(v.data(), length2*sizeof(Vec), MPI_BYTE, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // for(int i=0;i<length2;i++)
            // {
            //     printf("%d %d %c\n",v[i].x,v[i].y,v[i].d);
            // }
            int num[nrows][N];
            for (int i = 0; i < nrows; i++)
            {
                for (int j = 0; j < N; j++)
                {
                    num[i][j] = 0;
                }
            }
            for (int i = 0; i < v.size(); i++)
            {
                num[v[i].y - nr * rank][v[i].x]++;
            }
            for (int i = 0; i < T; i++)
            {
                int sending;
                int rec;
                vector<Vec> send1;
                

                if (rank != size - 1)
                {
                    int sss = 0;
                    
                    for (int o = 0; o < v.size(); o++)
                    {
                        if (v[o].y >= nr * (rank + 1))
                        {
                            sss++;
                            send1.push_back(v[o]);
                            num[v[o].y - nr * rank][v[o].x]--;
                            v.erase(v.begin() + o);
                        }
                    }

                    MPI_Send(&sss, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD);
                    if(sss>0)
                    {
                        MPI_Send(send1.data(),sss* sizeof(Vec), MPI_BYTE, rank+1, 1, MPI_COMM_WORLD);

                    }
                    send1.clear();

                    
                }

                            
                MPI_Recv(&rec, 1, MPI_INT, rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                // printf("hhh  %d\n",rec);
                vector<Vec> recv1(rec);
                if(rec>0)
                {
                    MPI_Recv(recv1.data(), rec*sizeof(Vec), MPI_BYTE, rank-1, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                }
                // printf("out \n");
                for (int f = 0; f < rec; f++)
                {
                    
                    v.push_back(recv1[f]);
                    num[recv1[f].y-nr*rank][recv1[f].x]++;
                }
                recv1.clear();


                int sending2=0;
                int rec2=0;
                vector<Vec>send2;
                
                for (int o = 0; o < v.size(); o++)
                {
                    if (v[o].y < nr * (rank))
                    {
                        sending2++;
                        send2.push_back(v[o]);
                        num[v[o].y - nr * rank][v[o].x]--;
                        v.erase(v.begin() + o);
                    }
                }
                MPI_Send(&sending2, 1, MPI_INT, rank - 1, 0, MPI_COMM_WORLD);
                if(sending2>0)
                {
                    MPI_Send(send2.data(),sending2* sizeof(Vec), MPI_BYTE, rank-1, 1, MPI_COMM_WORLD);


                }
                send2.clear();
                if(rank!=size-1)
                {
                    
                    MPI_Recv(&rec2, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    vector<Vec>recv2(rec2);
                    if(rec2>0)
                    {
                        MPI_Recv(recv2.data(), rec2*sizeof(Vec), MPI_BYTE, rank+1, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                    }
                    for (int f = 0; f < rec2; f++)
                    {
                        
                        v.push_back(recv2[f]);
                        num[recv2[f].y-nr*rank][recv2[f].x]++;
                    }
                    recv2.clear();

                    

                }
                map<pair<int, int>, vector<char>> mp;
                for (int m = 0; m < v.size(); m++)
                {
                    
                    if (num[v[m].y - nr * rank][v[m].x] == 2)
                    {
                        // if(i==64)
                        // {
                        //     printf("yayyy\n");
                        // }
                        mp[{v[m].y, v[m].x}].push_back(v[m].d);
                    }
                }

                for (int j = 0; j < v.size(); j++)
                {
                    int xaxis = v[j].x;
                    int yaxis = v[j].y;
                    // if(i==T-1)
                    // {
                    //      printf("%d %d num  %d %d\n",rank,num[v[j].y-nr*rank][v[j].x],v[j].x,v[j].y);
                    // }
                    // if(i==63)
                    // {
                    //     printf("%d kkk\n",num[7][5]);
                    // }

                    if (num[v[j].y - nr * rank][v[j].x] == 2 && xaxis != 0 && xaxis != N - 1 && yaxis != 0 && yaxis != M - 1)
                    {
                        // printf("jjjjjjjjjjjj\n");
                        // printf("%d  hhhh",i);

                        // printf("jj");
                        // printf("jj ");
                        int cout = 0;
                        // num[v[j].y][v[j].x]--;
                        if (mp[{v[j].y, v[j].x}][0] == 'L' && mp[{v[j].y, v[j].x}][1] == 'R' && cout == 0)
                        {
                            cout = 1;
                            if (v[j].d == 'L')
                            {
                                v[j].d = 'D';
                            }
                            if (v[j].d == 'R')
                            {
                                v[j].d = 'U';
                            }
                        }
                        if (mp[{v[j].y, v[j].x}][0] == 'R' && mp[{v[j].y, v[j].x}][1] == 'L' && cout == 0)
                        {
                            cout = 1;
                            if (v[j].d == 'L')
                            {
                                v[j].d = 'D';
                            }
                            if (v[j].d == 'R')
                            {
                                v[j].d = 'U';
                            }
                        }
                        if (mp[{v[j].y, v[j].x}][0] == 'U' && mp[{v[j].y, v[j].x}][1] == 'D' && cout == 0)
                        {
                            cout = 1;
                            if (v[j].d == 'U')
                            {
                                v[j].d = 'L';
                            }
                            if (v[j].d == 'D')
                            {
                                v[j].d = 'R';
                            }
                        }
                        if (mp[{v[j].y, v[j].x}][0] == 'D' && mp[{v[j].y, v[j].x}][1] == 'U' && cout == 0)
                        {
                            cout = 1;
                            if (v[j].d == 'U')
                            {
                                v[j].d = 'L';
                            }
                            if (v[j].d == 'D')
                            {
                                v[j].d = 'R';
                            }
                        }
                    }
                }
                for (int j = 0; j < v.size(); j++)
                {
                    int cout = 0;

                    if (v[j].d == 'R' && cout == 0)
                    {
                        cout = 1;
                        int r = v[j].x + 1;
                        if (r > (N - 1))
                        {
                            num[v[j].y - nr * rank][v[j].x]--;
                            v[j].x--;
                            v[j].d = 'L';
                            // printf("jj");
                            // printf("%c ",v[j].d);
                            num[v[j].y - nr * rank][v[j].x]++;
                        }
                        else
                        {
                            num[v[j].y - nr * rank][v[j].x]--;
                            v[j].x++;
                            num[v[j].y - nr * rank][v[j].x]++;
                        }
                    }
                    if (v[j].d == 'L' && cout == 0)
                    {
                        cout = 1;
                        int l = v[j].x - 1;
                        if (l < 0)
                        {
                            num[v[j].y - nr * rank][v[j].x]--;
                            v[j].x++;
                            v[j].d = 'R';
                            num[v[j].y - nr * rank][v[j].x]++;
                        }
                        else
                        {
                            num[v[j].y - nr * rank][v[j].x]--;
                            v[j].x--;
                            num[v[j].y - nr * rank][v[j].x]++;
                        }
                    }
                    if (v[j].d == 'U' && cout == 0)
                    {
                        cout = 1;
                        int u = v[j].y + 1;
                        if (u > (M - 1))
                        {
                            num[v[j].y - nr * rank][v[j].x]--;
                            v[j].y--;
                            v[j].d = 'D';
                            num[v[j].y - nr * rank][v[j].x]++;
                        }
                        else
                        {
                            num[v[j].y - nr * rank][v[j].x]--;
                            v[j].y++;
                            num[v[j].y - nr * rank][v[j].x]++;
                        }
                    }

                    if (v[j].d == 'D' && cout == 0)
                    {
                        cout = 1;
                        int u = v[j].y - 1;
                        if (u < 0)
                        {
                            num[v[j].y - nr * rank][v[j].x]--;
                            v[j].y++;
                            v[j].d = 'U';
                            num[v[j].y - nr * rank][v[j].x]++;
                        }
                        else
                        {
                            num[v[j].y - nr * rank][v[j].x]--;
                            v[j].y--;
                            num[v[j].y - nr * rank][v[j].x]++;
                        }
                    }
                }
                mp.clear();
                
                
            }
            int plen=v.size();
            MPI_Send(&plen, 1, MPI_INT,0, 0, MPI_COMM_WORLD);
            if(plen>0)
            {
                MPI_Send(v.data(), plen* sizeof(Vec), MPI_BYTE,0, 1, MPI_COMM_WORLD);
            }

                
            



        }
    }
     MPI_Barrier( MPI_COMM_WORLD );
    double elapsedTime = MPI_Wtime() - tbeg;
    double maxTime;
    MPI_Reduce( &elapsedTime, &maxTime, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD );
    if ( rank == 0 ) {
        printf( "Total time (s): %f\n", maxTime );
    }
    MPI_Finalize();
}
