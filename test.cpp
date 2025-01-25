#include <iostream>
#include <vector>
#include <queue>
#include <bits/stdc++.h>

using namespace std;

const int MAX_NODES = 100001; // Adjust as needed

vector<int> adj[MAX_NODES];
vector<bool> visited(MAX_NODES, false);
vector<int> dist(MAX_NODES, -1); // Changed variable name to 'dist'

void bfs(int source) {
    queue<int> q;
    q.push(source);
    visited[source] = true;
    dist[source] = 0; 

    while (!q.empty()) {
        int u = q.front();
        q.pop();

        for (int v : adj[u]) {
            if (!visited[v]) {
                visited[v] = true;
                dist[v] = dist[u] + 1; 
                q.push(v);
            }
        }
    }
}

int main() {
    // take input from a file
    freopen("input.txt", "r", stdin);
    int N, M;
    cin >> N >> M;
    for (int i = 0; i < M; i++) {
        int u, v, d;
        cin >> u >> v >> d;
        if (d == 1) { // Bidirectional
            adj[u].push_back(v);
            adj[v].push_back(u);
        } else { // Unidirectional
            adj[v].push_back(u); // Reverse the edge for BFS from R
        }
    }

    int k;
    cin >> k;

    vector<int> start_nodes(k);
    for (int i = 0; i < k; i++) {
        cin >> start_nodes[i];
    }

    int R;
    cin >> R;

    int L;
    cin >> L;

    vector<int> blocked_chambers(L);
    for (int i = 0; i < L; i++) {
        cin >> blocked_chambers[i];
        // Mark blocked chambers as visited to prevent exploration
        visited[blocked_chambers[i]] = true; 
    }
    
    // Close the file
    fclose(stdin);

    bfs(R);

    // Output to a file
    freopen("output.txt", "w", stdout);
    for (int i = 0; i < k; i++) {
        cout << dist[start_nodes[i]] << " ";
    }
    cout << endl;
    fclose(stdout);

    return 0;
}