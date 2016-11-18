# reverseproxy

A streaming HTTP Reverse proxy in Golang. Useful for cases where you need to ferry large request/response body to the server but with limited memory.

The reverse proxy reads the HTTP Body (request/response) in blocks of a fixed size eg. 512K.
Users can register callbacks for 
  - HTTP Request
  - HTTP Response
  - HTTP Request Body: the callback is invoked for each block.
  - HTTP Response Body: the callback is invoked for each block.

Use cases:
1. Use reverse proxy for load balancing
2. Use reverse proxy for adding/modifying/deleting http request/response headers
3. Use reverse proxy to modify the Request/Response data. For example, encrypt data
4. Use reverse proxy to inspect data (read only)


