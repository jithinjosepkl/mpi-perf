#include <mpi.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <time.h>

#define MAX_HOST_SZ (128)
#define DEF_BUF_SZ (456131)
#define DEF_ITERS (10)
#define LOG_REFRESH_TIME_SEC (900)

int world_size, world_rank, node_local_rank;

struct node_info
{
    int group_id;
    int group_rank;
    int rank;
    char hostname[MAX_HOST_SZ];
    char ipaddress[MAX_HOST_SZ];
};

int my_strnicmp(const char* s1, const char* s2, size_t n)
{
    int result = 0;
    for (size_t i = 0; i < n; i++)
    {
        int c1 = tolower((unsigned char)s1[i]);
        int c2 = tolower((unsigned char)s2[i]);

        if (c1 != c2)
        {
            result = c1 - c2;
            break;
        }
        else if (c1 == '\0')
        {
            break;
        }
    }
    return result;
}

#define MPI_CHECK(stmt)                                          \
do {                                                             \
   int mpi_errno = (stmt);                                       \
   if (MPI_SUCCESS != mpi_errno) {                               \
       fprintf(stderr, "[%s:%d] MPI call failed with %d \n",     \
        __FILE__, __LINE__,mpi_errno);                           \
       exit(EXIT_FAILURE);                                       \
   }                                                             \
   assert(MPI_SUCCESS == mpi_errno);                             \
} while (0)


void do_mpi_benchmark_unidir(struct node_info *my_node_info, struct node_info *peer_node_info,
    int iters, void* buffer_tx, void* buffer_rx, int buff_len)
{
    MPI_Status status;

    for (int i = 0; i < iters; i++)
    {
        if (my_node_info->group_id == 1)
        {
            MPI_CHECK(MPI_Send(buffer_tx, buff_len, MPI_CHAR, peer_node_info->rank, 1, MPI_COMM_WORLD));
            MPI_CHECK(MPI_Recv(buffer_rx, 1, MPI_CHAR, peer_node_info->rank, 2, MPI_COMM_WORLD, &status));
        }
        else
        {
            MPI_CHECK(MPI_Recv(buffer_rx, buff_len, MPI_CHAR, peer_node_info->rank, 1, MPI_COMM_WORLD, &status));
            MPI_CHECK(MPI_Send(buffer_tx, 1, MPI_CHAR, peer_node_info->rank, 2, MPI_COMM_WORLD));
        }
    }
}

void get_ipaddress(char* hostname, char* ipstr)
{
    struct addrinfo hints, * res;
    int status;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if ((status = getaddrinfo(hostname, NULL, &hints, &res)) != 0)
    {
        fprintf(stderr, "getaddrinfo error: %ls\n", gai_strerror(status));
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    void* addr;
    // loop through all the results and get the address
    for (struct addrinfo* p = res; p != NULL; p = p->ai_next)
    {
        struct sockaddr_in* ipv4 = (struct sockaddr_in*)p->ai_addr;
        addr = &(ipv4->sin_addr);

        // convert the IP to a string and print it:
        inet_ntop(p->ai_family, addr, ipstr, INET_ADDRSTRLEN);
    }

    freeaddrinfo(res); // free the linked list
}

void get_peer_info(int my_group, int group_rank, char* myhostname, char* myipaddress,
    struct node_info* world_node_info, struct node_info** p_my_node_info, struct node_info** p_peer_node_info)
{
    struct node_info my_node_info = { 0 };
    my_node_info.group_id = my_group;
    my_node_info.group_rank = group_rank;
    my_node_info.rank = world_rank;

    memcpy(my_node_info.hostname, myhostname, strlen(myhostname));
    memcpy(my_node_info.ipaddress, myipaddress, strlen(myipaddress));

    MPI_Allgather(&my_node_info, sizeof(struct node_info), MPI_BYTE,
        world_node_info, sizeof(struct node_info), MPI_BYTE, MPI_COMM_WORLD);

    *p_my_node_info = (struct node_info*)world_node_info + world_rank;
    for (int i = 0; i < world_size; i++)
    {
        struct node_info* info = (struct node_info*)world_node_info + i;
        if (info->group_id != my_group && info->group_rank == group_rank)
        {
            *p_peer_node_info = info;
        }
    }
}

void posix_memalign(void** buffer, size_t alignment, size_t buffer_sz)
{
    *buffer = _aligned_malloc(buffer_sz, alignment);
}

void allocate_tx_rx_buffers(void** buffer_tx, void** buffer_rx, int buff_len, int my_group)
{
    posix_memalign(buffer_tx, 4096, buff_len);
    posix_memalign(buffer_rx, 4096, buff_len);
    if (my_group == 0)
    {
        memset(*buffer_tx, 'a', buff_len);
    }
    else
    {
        memset(*buffer_tx, 'b', buff_len);
    }
}

char group1_hostfile[128] = { 0 };
int group_size = 0;

struct options
{
    int use_dotnet;
    int iters;
    int buff_sz;
    int uni_dir;
    int num_runs;
    int ppn;
    int nonblocking;
    char uuid[64];
    char logfolder[MAX_HOST_SZ];
};

struct options bench_options = { 0 };
FILE* log_fp = NULL;



void generate_uuid(char * guid_str)
{
    GUID guid;
    CoCreateGuid(&guid);

    snprintf(guid_str, sizeof(guid_str),
        "%08lx-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        guid.Data1, guid.Data2, guid.Data3,
        guid.Data4[0], guid.Data4[1], guid.Data4[2], guid.Data4[3],
        guid.Data4[4], guid.Data4[5], guid.Data4[6], guid.Data4[7]);
}

void parse_args(int argc, char** argv)
{
    strncpy(group1_hostfile, argv[1], MAX_HOST_SZ);
    group_size = (int)atoi(argv[2]);
    bench_options.ppn = (int)atoi(argv[3]);
    bench_options.iters = (int)atoi(argv[4]);
    bench_options.buff_sz = (int)atoi(argv[5]);
    bench_options.num_runs = (int)atoi(argv[6]);
    strncpy(bench_options.logfolder, argv[7], MAX_HOST_SZ);
    generate_uuid(&bench_options.uuid[0]);
}

void getformatted_time(char* buffer, int for_kusto)
{
    time_t t = time(NULL);
    struct tm tm = *localtime(&t);

    char buf[64];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);

    if (for_kusto)
        strftime(buffer, MAX_HOST_SZ, "%Y-%m-%d %H:%M:%S", &tm);
    else
        strftime(buffer, MAX_HOST_SZ, "%Y-%m-%d-%H-%M-%S", &tm);
}

int main(int argc, char** argv)
{
    int i = 0;
    int my_group = 0, group_rank = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm group_comm;

    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    node_local_rank = world_rank / 8;
    char* group1_ipaddresses = NULL;

    bench_options.use_dotnet = 0;
    bench_options.uni_dir = 1;
    bench_options.iters = DEF_ITERS;
    bench_options.buff_sz = DEF_BUF_SZ;
    bench_options.num_runs = 1;

    if (world_rank == 0)
    {
        parse_args(argc, argv);

        // validate group_size
        if (group_size <= 0 || (!bench_options.uni_dir && group_size != world_size / (2 * bench_options.ppn)))
        {
            fprintf(stderr, "invalid group_size: %d, world_size: %d, ppn: %d\n", group_size, world_size, bench_options.ppn);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }

        // read group1 hostnames
        group1_ipaddresses = (char*)malloc(group_size * MAX_HOST_SZ);
        memset(group1_ipaddresses, 0, group_size * MAX_HOST_SZ);

        FILE* fptr = NULL;
        fptr = fopen(group1_hostfile, "r");
        if (fptr == NULL)
        {
            fprintf(stderr, "cannot open group1 file: %s\n", group1_hostfile);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }

        while (fgets(group1_ipaddresses + i * MAX_HOST_SZ, MAX_HOST_SZ, fptr))
        {
            char* buffer = group1_ipaddresses + i * MAX_HOST_SZ;
            buffer[strcspn(buffer, "\n")] = '\0';
            i++;
        }
    }

    // broadcase benchmark options
    MPI_Bcast(&bench_options, sizeof(bench_options), MPI_CHAR, 0, MPI_COMM_WORLD);

    // broadcast group1 hosts info to all other processes
    MPI_Bcast(&group_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (world_rank != 0)
    {
        group1_ipaddresses = (char*)malloc(group_size * MAX_HOST_SZ);
        memset(group1_ipaddresses, 0, group_size * MAX_HOST_SZ);
    }
    MPI_Bcast(group1_ipaddresses, group_size * MAX_HOST_SZ, MPI_CHAR, 0, MPI_COMM_WORLD);

    int name_len;
    char myhostname[MAX_HOST_SZ] = { 0 };
    char my_ipaddr[MAX_HOST_SZ] = { 0 };
    MPI_Get_processor_name(myhostname, &name_len);
    get_ipaddress(myhostname, &my_ipaddr[0]);
    
    // identify if i am in group1 or not
    for (int i = 0; i < group_size; i++)
    {
        if (my_strnicmp(my_ipaddr, group1_ipaddresses + i * MAX_HOST_SZ, MAX_HOST_SZ) == 0)
        {
            my_group = 1;
        }
    }

    // Create a new communicator consisting of processes with the same group
    MPI_Comm_split(MPI_COMM_WORLD, my_group, world_rank, &group_comm);

    MPI_Comm_size(group_comm, &group_size);
    MPI_Comm_rank(group_comm, &group_rank);

    struct node_info* my_node_info = NULL;
    struct node_info* peer_node_info = NULL;
    struct node_info* world_node_info = (struct node_info*)malloc(sizeof(struct node_info) * world_size);
    memset(world_node_info, 0, sizeof(struct node_info) * world_size);
    get_peer_info(my_group, group_rank, myhostname, my_ipaddr, world_node_info, &my_node_info, &peer_node_info);

    fprintf(stdout, "INFO: %s, rank %d out of %d ranks, my_group: %d, group_size: %d, group_rank: %d, my_peer: %d, hostname: %s (%s), peer_host: %s (%s)\n",
        my_node_info->hostname, world_rank, world_size, my_node_info->group_id, group_size, group_rank,
        peer_node_info->rank, &my_node_info->hostname[0], &my_node_info->ipaddress[0], &peer_node_info->hostname[0], &peer_node_info->ipaddress[0]);
    fflush(stdout);

    void* buffer_tx, * buffer_rx;
    int buff_len = bench_options.buff_sz;
    allocate_tx_rx_buffers(&buffer_tx, &buffer_rx, buff_len, my_group);

    // core benchmark
    double t_last_logtime = 0.0;
    t_last_logtime = MPI_Wtime();

    for (long long int run_idx = 0; bench_options.num_runs == -1 || run_idx < bench_options.num_runs; run_idx++)
    {
        double t_start = 0.0, t_end = 0.0, t_end_local = 0.0;
        double my_time, min_time, max_time, sum_time;

        if (my_group == 1 && (log_fp == NULL || ((MPI_Wtime() - t_last_logtime) > LOG_REFRESH_TIME_SEC)))
        {
            char fileName[2 * MAX_HOST_SZ] = { 0 };

            if (log_fp != NULL) {
                fflush(log_fp);
                fclose(log_fp);

            }

            char formatted_time[26] = { 0 };
            getformatted_time(formatted_time, 0);
            sprintf(fileName, "%s/tcp-%s-%d-%s.log", bench_options.logfolder, bench_options.uuid, world_rank, formatted_time);
            log_fp = fopen(fileName, "w");
            t_last_logtime = MPI_Wtime();
        }

        MPI_Barrier(MPI_COMM_WORLD);

        t_start = MPI_Wtime();

        do_mpi_benchmark_unidir(my_node_info, peer_node_info, bench_options.iters, buffer_tx, buffer_rx, buff_len);

        t_end_local = MPI_Wtime();
        my_time = t_end_local - t_start;

        // generate data for kusto ingestion; dotnet benchmark reports this inside the dotnet benchmark
        if (bench_options.use_dotnet == 0 && run_idx > 0 && my_group == 1)
        {
            char formatted_time[MAX_HOST_SZ] = { 0 };
            getformatted_time(formatted_time, 1);

            // format: Timestamp:datetime,JobId:string,Rank:int,VMCount:int,LocalIP:string,RemoteIP:string,NumOfFlows:int,BufferSize:int,NumOfBuffers:int,TimeTakenms:real,RunId:int
            fprintf(log_fp, "%s,%s,%d,%d,%s,%s,%d,%d,%d,%.2lf,%lld\n",
                formatted_time, bench_options.uuid, world_rank, world_size / bench_options.ppn,
                my_ipaddr, peer_node_info->ipaddress, bench_options.ppn, buff_len,
                bench_options.iters, my_time * 1000.0, run_idx);
        }

        MPI_Barrier(MPI_COMM_WORLD);
        t_end = MPI_Wtime();

        MPI_Allreduce(&my_time, &min_time, 1, MPI_DOUBLE, MPI_MIN, MPI_COMM_WORLD);
        MPI_Allreduce(&my_time, &max_time, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
        MPI_Allreduce(&my_time, &sum_time, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);

        if (world_rank == 0 && (run_idx % 1000 == 0))
        {
            fprintf(stderr, "[Run#: %lld]: Total time: %.2lf ms, Min: %.2lf ms, Max: %.2lf ms, Avg: %.2lf ms\n", run_idx,
                (t_end - t_start) * 1000.0, min_time * 1000.0, max_time * 1000.0, (sum_time * 1000) / world_size);
        }
    }

    if (log_fp != NULL)
        fclose(log_fp);

    free(world_node_info);
    _aligned_free(buffer_tx);
    _aligned_free(buffer_rx);

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
}
