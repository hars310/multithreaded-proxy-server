#include "proxy_parse.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/wait.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>

#define MAX_CLIENTS 16 // setting 16 just randomly


// we have to store the request data into cache element
// this is actually a lru cache list that will contain all our cache request data
struct cache_element{
    char* data;         // data stores response
    int len;          // length of data i.e.. sizeof(data)...
    char* url;        // url stores the request
	time_t lru_oldest_time;    // lru_oldest_time stores the latest time the element is  accesed
    struct cache_element* next; // this is the pointer that will point to next element in cache list
};

// methods present in this cache list
int addNewRequest(char* data, int size, char* url); // add a new request 
struct cache_element* findRequest(char* url); // find a element in this list
void deleteRequest();  // delete the request data from this list


int PORT = 4040; // port no where our proxy will work
int proxy_socket_Id;  // to maintain the connection we will have to use socket
pthread_t p_id[MAX_CLIENTS]; // this is the number of threads 
                            //  for ex -  we can have more than 1 clients who is using this proxy server at same time 

// Semaphore to ensure that only MAX_CLIENTS number of clients can be handled concurrently.
// If more clients try to connect, their threads will wait (block) until a slot is available.
sem_t semaphore; 

// this lock is like a bool datatype that will return 0 or 1
// in case some client is writing some changes in linked list 
// at the same time if some other client tries to write at same time 
// to avoid this thing we use locks to have control over our data integrity
pthread_mutex_t lock;


int cache_size;
struct cache_element* head;

int main(int argc, char* argv[]){
    int client_socketID; 
    int client_len; // length of address
    
    // I used this by my own
    // this helps to assign addresses to client and server
    // older method --> struct sockaddr server_addr, client_addr; --> this does not have fields for ip address and port directly
    // older method was too generic that's why i thought to try for ipv4 addresses
    struct sockaddr_in server_addr, client_addr;

    
    // initialisation of semaphore with min and max value
    sem_init(&semaphore,0, MAX_CLIENTS);

    // thread initialisation
    pthread_mutex_init(&lock, NULL);


    // argc checks how many arguments we have passed in command line
    // for this we will run our server like this ./server 8080
    // port number is passes as second element of argument arr
    if(argc == 2){
        PORT = atoi(argv[1]);
    }
    else{
        printf("Few arguments passed");
        exit(1);
    }

    // in this socket id we pass a socket value that is generated using socket function
    // here AF_NET(address family: internet) is domain
    // more sepicfically speaking it will use ipv4 addresses
    // and we set sock stream as the type beacuse it will represent the stream socket
    // and uses TCP under the hood for communication
    // for udp type connection we can use SOCK_DGRAM
    proxy_socket_Id = socket(AF_INET,SOCK_STREAM,0);

    if(proxy_socket_Id < 0) {
        perror("Failed to create a socket");
        exit(1);    
    }

    // this reuse will help us to reuse the socket as it will be a global socket
    int reuse = 1;

    // this setsockopt fn is used to set the socket options like timeouts, buffer sizes, or address reuse.
    // this setsockopt fn takes 5 parameters (sockfd--> Socket file descriptor, level, optionname, optionvalue, optionlength)
    // SO_REUSEADDR ->  Allows your socket to reuse the same local address and port, even if they’re in a TIME_WAIT state.
    // if this setsockopt()<0 --> return error
    // this setsockopt is a wrapper around a kernel level system call
    // when we call it
            // --> it validates the socket descripter
            // --> Checks if the option is supported for the given level.
            // --> Applies the option by modifying socket internal configuration.
    if(setsockopt(
        proxy_socket_Id, 
        SOL_SOCKET,
        SO_REUSEADDR, 
        (const char*)&reuse, 
        sizeof(reuse)
    ) < 0 ){
            // If the call fails, print an error message and optionally exit
            perror("setsockopt(SO_REUSEADDR) failed");
            exit(EXIT_FAILURE);
    }

    // this function will set 0 value to server_addr as in c lang variables get garbage value in definition.
    // first minor doubt comes is why dont we directly place 0 to it, but it has diff things internally that need to be initialised with 0 as well
        // server_addr.sin_family = 0;
        // server_addr.sin_port = 0;
        // server_addr.sin_addr.s_addr = 0;
        // and so on...
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT); // converts the port to network byte order
    server_addr.sin_addr.s_addr = INADDR_ANY;  // server will accept connections from any IP on the machine.


    // bind function binds the socket with specific ip addr and port
    // it tells the os that Whenever data arrives on this port, send it to this socket.
    // without bind our os will never know which port our socket is using
    if (bind(proxy_socket_Id, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0){
        perror("PORT is unavailable\n");
        exit(1); 
    }

}