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

int PORT = 4040; // port no where our proxy will work
int proxy_socket_Id;  // to maintain the connection we will have to use socket
pthread_t p_id[MAX_CLIENTS]; // this is the number of threads 
                            //  for ex -  we can have more than 1 clients who is using this proxy server at same time 


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
int addNewRequestinCache(char* data, int size, char* url); // add a new request 
struct cache_element* findRequest(char* url); // find a element in this list
void deleteRequest();  // delete the request data from this list


int connectRemoteServer(char* host_addr,int port_num ){
    // Step 1: Create a TCP socket
    // AF_INET     → IPv4 address family
    // SOCK_STREAM → TCP socket (reliable, connection-oriented)
    // 0           → Protocol automatically chosen (TCP for SOCK_STREAM)
    int remoteSocket = socket(AF_INET,SOCK_STREAM,0);
    if(remoteSocket < 0){
        printf("Error in creating the socket\n");
        return -1;
    }

    // Step 2: Resolve the hostname to an IP address using DNS
    // Example: "example.com" → "93.184.216.34"
    struct hostent* host = gethostbyname(host_addr);
     if (host == NULL) {
        printf("DNS resolution failed for host: %s\n", host_addr);
        close(remoteSocket);
        return -1; // Cannot resolve hostname
    }

    // Step 3: Prepare the server address structure for connection
    struct sockaddr_in server_addr;
    memset((char*) &server_addr,0,sizeof(server_addr));
    server_addr.sin_family = AF_INET;               // IPv4
    server_addr.sin_port = htons(port_num);         // Convert port to network byte order
    memcpy((char *)host->h_addr_list[0] ,(char*) &server_addr.sin_addr.s_addr, host->h_length); // Copy IP address

    // Step 4: Attempt to connect to the server
    if (connect(remoteSocket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        printf("Error connecting to server %s:%d\n", host_addr, port_num);
        close(remoteSocket);
        return -1; // Connection failed
    }

    // Step 5: Return the connected socket descriptor
    return remoteSocket;
}


// int handle_request(int clientSocketId,struct ParsedRequest* request, char* temp_Request ){

//     /*
    
//     handle_request() – Process a client HTTP request and forward to server
//     -----------------------------------------------------------------
//     Steps:
//     1. Allocate buffer for building the outgoing HTTP request.
//     2. Construct the HTTP request line safely using snprintf.
//     3. Ensure required headers are set:
//        - Connection: close
//        - Host: <hostname>
//     4. Append all headers to the request buffer (unparse headers).
//     5. Determine target port (default 80 unless specified in request).
//     6. Connect to the remote server using host and port.
//     7. Send the complete HTTP request to the remote server.
//     8. Allocate a temporary buffer to store the full server response for caching.
//     9. Loop:
//          a) Receive a chunk from the remote server.
//          b) Forward the chunk to the client.
//          c) Append the chunk to the cache buffer.
//          d) Expand cache buffer if needed.
//     10. After all data is received, save full response in cache.
//     11. Free all allocated memory and close the remote server connection.
//     12. Return success (0) or error (-1).
//     */

//     // STEP 1: 
//     char* buf = (char*) malloc(sizeof(char)*MAX_BYTES);

//     if (!buf) return -1;
    
//     // older version -> gives error of bounding of buffer memory
//     // strcpy(buf, "GET");
//     // strcat(buf, request->path);
//     // strcat(buf, " ");
//     // strcat(buf, request->version);
//     // strcat(buf, "\r\n");
    
//     // STEP 2:
//     // new way to safely construct the request line
//     int written = snprintf(buf, MAX_BYTES, "GET%s %s\r\n", request->path, request->version);
//     if (written < 0 || written >= MAX_BYTES) {
//         fprintf(stderr, "Request line too long\n");
//         free(buf);
//         return -1;
//     }
//     size_t len = strlen(buf);

//      // STEP 3:
//     // After this request is done, close the TCP connection
//     if(ParsedHeader_set(request,"Connection","close")<0){
//         printf("Set header files are not working");
//     }

//     // Checks if the Host header is already present
//     if(ParsedHeader_get(request,"Host")==NULL){
//         ParsedHeader_set(request,"Host",request->host);
//     }

//     // STEP 4:
//     // Takes all the headers stored in request and writes them into the buffer buf starting from buf+len.
//     // "Unparse" here means convert the internal header representation back into raw HTTP header text like:
//     // Connection: close\r\n
//     // Host: example.com\r\n
//     if(ParsedRequest_unparse_headers(request, buf+len,(size_t)MAX_BYTES-len)<0){
//         printf("Unparsing failed");
//     }
    
//     // STEP 5:
//     int port = 80;
//     if(request->port != NULL){
//         PORT = atoi(request->port);

//     }

//      // STEP 6:
//     // create a TCP connection to the given host and port so you can send the request
//     int remoteSocketId = connectRemoteServer(request->host, PORT);
//     if (remoteSocketId < 0) {
//     fprintf(stderr, "Failed to connect to remote server: %s:%d\n", request->host, PORT);
//     free(buf); // free any allocated resources
//     return -1; // stop processing, cannot proceed without connection
//     }

//     // STEP 7:
//     int bytes_send = send(remoteSocketId,buf,strlen(buf),0);
//     if (bytes_send < 0) {
//     perror("Error sending request to remote server");
//     free(buf);
//     return -1;
//     }
//     memset(buf,0,MAX_BYTES);
    
//     // STEP 8:
//     bytes_send = recv(remoteSocketId,buf,MAX_BYTES-1,0);
//     char* temp_buffer = (char*) malloc(sizeof(char)* MAX_BYTES);
//     int temp_buffer_size = MAX_BYTES;
// 	int temp_buffer_index = 0;

//     // STEP 9:
//     while(bytes_send > 0)
// 	{
// 		bytes_send = send(clientSocketId, buf, bytes_send, 0);
		
// 		for(int i=0;i<bytes_send/sizeof(char);i++){
// 			temp_buffer[temp_buffer_index] = buf[i];
// 			// printf("%c",buf[i]); // Response Printing
// 			temp_buffer_index++;
// 		}
// 		temp_buffer_size += MAX_BYTES;
// 		temp_buffer=(char*)realloc(temp_buffer,temp_buffer_size);

// 		if(bytes_send < 0)
// 		{
// 			perror("Error in sending data to client socket.\n");
// 			break;
// 		}
// 		memset(buf,0, MAX_BYTES);

// 		bytes_send = recv(remoteSocketId, buf, MAX_BYTES-1, 0);

// 	} 
//     // STEP 10:
// 	addNewRequestinCache(temp_buffer, strlen(temp_buffer), temp_Request);
//     temp_buffer[temp_buffer_index]='\0';

// 	free(buf);
// 	printf("Done\n");
// 	free(temp_buffer);
	
	
//  	close(remoteSocketId);
// 	return 0;
// }


int handle_request(int clientSocketId, struct ParsedRequest* request, char* temp_Request) {
    /*
    // used chatgpt to write steps for my code just to understand it more easily 
    =====================================================================
    handle_request() – Process a client HTTP request and forward to server
    =====================================================================
    Steps:
    1. Allocate buffer for building the outgoing HTTP request.
    2. Construct the HTTP request line safely using snprintf.
    3. Ensure required headers are set:
       - Connection: close
       - Host: <hostname>
    4. Append all headers to the request buffer (unparse headers).
    5. Determine target port (default 80 unless specified in request).
    6. Connect to the remote server using host and port.
    7. Handle connection failure.
    8. Send the complete HTTP request to the remote server.
    9. Allocate a temporary buffer to store the full server response for caching.
    10. Loop:
         a) Receive a chunk from the remote server.
         b) Forward the chunk to the client.
         c) Append the chunk to the cache buffer.
         d) Expand cache buffer if needed.
    11. After all data is received, save full response in cache.
    12. Free all allocated memory and close the remote server connection.
    13. Return success (0) or error (-1).
    =====================================================================
    */

    // STEP 1: Allocate buffer for building the outgoing HTTP request
    char* buf = (char*) malloc(sizeof(char) * MAX_BYTES);
    if (!buf) return -1;

    // STEP 2: Construct the HTTP request line safely using snprintf
    int written = snprintf(buf, MAX_BYTES, "GET%s %s\r\n", request->path, request->version);
    if (written < 0 || written >= MAX_BYTES) {
        fprintf(stderr, "Request line too long\n");
        free(buf);
        return -1;
    }
    size_t len = strlen(buf);

    // STEP 3: Ensure required headers are set: Connection and Host
    if (ParsedHeader_set(request, "Connection", "close") < 0) {
        printf("Set header files are not working\n");
    }
    if (ParsedHeader_get(request, "Host") == NULL) {
        ParsedHeader_set(request, "Host", request->host);
    }

    // STEP 4: Append all headers to the request buffer (unparse headers)
    if (ParsedRequest_unparse_headers(request, buf + len, (size_t)MAX_BYTES - len) < 0) {
        printf("Unparsing failed\n");
    }

    // STEP 5: Determine target port (default 80)
    int port = 80;
    if (request->port != NULL) {
        port = atoi(request->port);
    }

    // STEP 6: Connect to the remote server using host and port
    int remoteSocketId = connectRemoteServer(request->host, port);

    // STEP 7: Handle connection failure
    if (remoteSocketId < 0) {
        fprintf(stderr, "Failed to connect to remote server: %s:%d\n", request->host, port);
        free(buf);
        return -1;
    }

    // STEP 8: Send the complete HTTP request to the remote server
    int bytes_sent = send(remoteSocketId, buf, strlen(buf), 0);
    if (bytes_sent < 0) {
        perror("Error sending request to remote server");
        free(buf);
        close(remoteSocketId);
        return -1;
    }
    memset(buf, 0, MAX_BYTES);

    // STEP 9: Allocate a temporary buffer to store the full server response for caching
    int bytes_received = recv(remoteSocketId, buf, MAX_BYTES - 1, 0);
    if (bytes_received < 0) {
        perror("Error receiving response from remote server");
        free(buf);
        close(remoteSocketId);
        return -1;
    }

    char* temp_buffer = (char*) malloc(sizeof(char) * MAX_BYTES);
    if (!temp_buffer) {
        fprintf(stderr, "Failed to allocate memory for temp_buffer\n");
        free(buf);
        close(remoteSocketId);
        return -1;
    }
    int temp_buffer_size = MAX_BYTES;
    int temp_buffer_index = 0;

    // STEP 10: Loop to receive from remote server, forward to client, and cache
    while (bytes_received > 0) {
        // Forward to client
        int sent_to_client = send(clientSocketId, buf, bytes_received, 0);
        if (sent_to_client < 0) {
            perror("Error sending data to client socket");
            break;
        }

        // Store in cache buffer
        for (int i = 0; i < bytes_received; i++) {
            temp_buffer[temp_buffer_index++] = buf[i];
        }

        // Expand temp_buffer if needed
        if (temp_buffer_index + MAX_BYTES > temp_buffer_size) {
            temp_buffer_size += MAX_BYTES;
            char* new_buffer = (char*) realloc(temp_buffer, temp_buffer_size);
            if (!new_buffer) {
                fprintf(stderr, "Failed to realloc temp_buffer\n");
                break;
            }
            temp_buffer = new_buffer;
        }

        memset(buf, 0, MAX_BYTES);
        bytes_received = recv(remoteSocketId, buf, MAX_BYTES - 1, 0);
        if (bytes_received < 0) {
            perror("Error receiving response from remote server");
            break;
        }
    }

    // STEP 11: Null-terminate and save full response in cache
    if (temp_buffer_index < temp_buffer_size) {
        temp_buffer[temp_buffer_index] = '\0';
    } else {
        // Ensure there is space for null terminator
        char* new_buffer = (char*) realloc(temp_buffer, temp_buffer_size + 1);
        if (new_buffer) {
            temp_buffer = new_buffer;
            temp_buffer[temp_buffer_size] = '\0';
        }
    }
    addNewRequestinCache(temp_buffer, strlen(temp_buffer), temp_Request);

    printf("Done\n");

    // STEP 12: Free allocated memory and close remote connection
    free(buf);
    free(temp_buffer);
    close(remoteSocketId);

    // STEP 13: Return success
    return 0;
}

// i used gpt to get this function so that i can send error message if got any error in http status
void send_error_message(int client_socket, const char* status, const char* message) {
    char response[1024];
    snprintf(response, sizeof(response),
        "HTTP/1.1 %s\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: %ld\r\n"
        "\r\n"
        "%s",
        status, strlen(message), message
    );
    send(client_socket, response, strlen(response), 0);
}



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
    // if the temp is not in cache list
    // some data has been received from the client (via recv() earlier).
    else if(bytes_send_client>0){
        len = strlen(buffer);
        // this is the http parsed  library method we initally added in code as "proxy_parse.c"
        struct ParsedRequest* request = ParsedRequest_create();
        
        // this internal method tries to parse
        if(ParsedRequest_parse(request, buffer, len) < 0 ){
            printf("Parsing failed");
        }else{
            memset(buffer, 0, MAX_BYTES);
            // we are writing code to handle get request only for now
            if(!strcmp(request->protocol,"GET")){
                // if both host and path is present
                if(request->host && request->path){

                    // this handle_request is to be written but it will forward the request
                    bytes_send_client = handle_request(socket, request, temp_Request);
                    if(bytes_send_client==-1){
                        send_error_message(socket, "500" , "Invalid http request"); 
                    }
                }else{
                    send_error_message(socket, "500", "Internal server error");
                }
            }else{
                printf("For now i have written only code for GET request\n");
            }
        }
        // Free the memory allocated for the ParsedRequest object.
        ParsedRequest_destroy(request);
    }
    else if(bytes_send_client==0)
    {
        printf("client is disconnected");
    }

    // close a socket connection.
    // SHUT_RDWR both reading and writing will be disabled
    shutdown(socket, SHUT_RDWR);

    // Clean up: Free buffer memory
    free(buffer);

    // Close the socket (important to avoid resource leaks)
    close(socket);

    // Release the semaphore (increment), allowing another thread to serve a new client
    sem_post(&semaphore);
    // sem_getvalue(&semaphore,p);

    free(temp_Request);

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