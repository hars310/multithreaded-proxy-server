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
#define MAX_BYTES 10*(1<<10) // 2^10


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

// implementation of thread fn to receive 
void* thread_fn(void* new_socket){
    // Wait (decrement) the semaphore to limit concurrent client handling
    // If the semaphore value is 0, this thread will wait until another thread releases it
    sem_wait(&semaphore);

    // this code is helpful to debug the code later
    int p;
    sem_getvalue(&semaphore, &p);
    printf("Semaphore value is %d\n",p);
    
    // first we are getting the socket details in an int ptr 
    // and then dereferencing to get the actual socket value in socket
    int* t = (int*) new_socket; 
    int socket = *t; // setting the socket detail in socket variable
    
    
    int bytes_send_client; // this is the size of data client is sending
    int len; 
    // buffer to recieve the client data
    char* buffer = (char*) calloc(MAX_BYTES, sizeof(char));
    
    // this is not needed as we have used calloc but incase we have garbage in buffer thats why;
    memset(buffer,0,MAX_BYTES);
    
    // recieving the client data 
    bytes_send_client = recv(socket,buffer,MAX_BYTES,0);


    while(bytes_send_client>0){
        len = strlen(buffer);
        
        // Check if the end of HTTP headers has not been reached yet
        if(strstr(buffer,"\r\n\r\n")==NULL){
            // Receive more data and append it to the end of the buffer
            bytes_send_client  =recv(socket, buffer + len, MAX_BYTES - len, 0);
        }else{
            break;
        }
    }

    // temp_Request is now a new char array in memory — a blank copy of buffer's size.
    // and copying the buffer each char to temp request in for loop
    // the reason i have done is simple, to avoid making any changes in original buffer
    char* temp_Request = (char*)malloc(strlen(buffer)*sizeof(char)+1);
    for(int i=0;i<strlen(buffer);i++){
        temp_Request[i] = buffer[i];
    }

    // now check that temp request in the cache list
    // if is is present then we can directly fetch the data from thatand send it to the socket
    struct cache_element* temp = findRequest(temp_Request);
    if(temp!=NULL){
        int size = temp->len/sizeof(char);
        int pos=0;
        char response[MAX_BYTES];
        while(pos<size){
            memset(response,0,MAX_BYTES);
            for(int i=0;i<MAX_BYTES;i++){
                // this temp contains the cache data
                // we are just putting it byte by byte in the response
                response[i] = temp->data[i];
                pos++;
            }

            // after successfully putting all the bytes we are sending this response to socket
            send(socket, response, MAX_BYTES, 0);
        }
        printf("Data retrieved successfully from the cache \n");
        printf("%s\n\n",response);
    }


    // Clean up: Free buffer memory
    free(buffer);

    // Close the socket (important to avoid resource leaks)
    close(socket);

    // Release the semaphore (increment), allowing another thread to serve a new client
    sem_post(&semaphore);

    // Exit the thread
    pthread_exit(NULL);

}

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
    printf("Binding on port %d\n",PORT);

    // listen() converts our bound socket into a listening socket that can accept incoming client connections.
    // the real life example to understand the code structure or flow is 
                // socket() → Get a phone.
                // bind() → Get a phone number and register it.
                // listen() → Start waiting for incoming calls.
                // accept() → Pick up a ringing phone call. -> after listening this has to be written
    int listen_status = listen(proxy_socket_Id,MAX_CLIENTS);
    if(listen_status<0){
        perror("Error in listening status. \n");
        exit(1);
    }

    int i=0;
    int connected_socketId[MAX_CLIENTS]; // array to store socket id that are connected

    while(1){
        // clears client address structure
        memset((char*)& client_addr,0,sizeof(client_addr));
        client_len = sizeof(client_addr);

        // Accept an incoming client connection
        // so question comes in mind if it is running infinite inside while loop
        // will it slow down the system --> ans is it does not consume unnecessary CPU or memory.
        // accept() is a blocking system call.
        // That means the process sits idle (sleeps) until a client actually tries to connect.
        // No CPU is wasted while waiting — the kernel wakes it up only when a connection comes in.
        client_socketID  = accept(proxy_socket_Id, (struct sockaddr*)& client_addr, (socklen_t*) &client_len);

        if(client_socketID < 0){
            perror("error in accept function");
            exit(1);
        }
        else{
            connected_socketId[i] = client_socketID;
            i++;
        }

        // making a ptr to point at the same address of client_addr
        struct sockaddr_in * client_ptr = &client_addr;
        
        // now getting ip addr from the ptr 
        struct in_addr ip_addr =  client_ptr->sin_addr;
        
        // creating a string to set the addr value
        // the size of this string is 16 with max addr can be 255.255.255.255
        char str[INET_ADDRSTRLEN];

        // it converts the internal addr to presentation format
        // converts a binary IP address (used by the system) to a human-readable IP string like "192.168.1.1".
        inet_ntop(AF_INET, &ip_addr, str, INET_ADDRSTRLEN);

        printf("Client is Connected with port number : %d and ip address: %s",ntohs(client_addr.sin_port),str);

        // now after we have connected with client and got their port and addr details 
        // create a thread for this client
        // this thread function is a implementation to receive the client bytes
        pthread_create(&p_id[i], NULL, thread_fn, (void*)& connected_socketId[i]);
    }
        
    close(proxy_socket_Id);


}