# MP1
First, on every virtual machine start server service for grep by running the server.go file
You can use the terminal and type : go run server.go

Then, you can start using the grep service on client by running the client.go
For a specific string, you can use the terminal and type : go run client.go Mozilla 
For a regular expression, you can use the terminal and type : go run client.go "'^[0-9]*[a-z]{5}'"

If you want to test, you should first install a Python package called xeger using pip install xeger.
Then, you can use the terminal and type : go test -v

If the terminal shows "The result is ok", it means the test passes.
On the contrary, if the terminal shows "The result is wrong", it means the test fails.
