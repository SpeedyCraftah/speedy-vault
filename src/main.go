package main

import (
	"log"
	"speedyvault/src/handlers"
	"speedyvault/src/routes"

	"github.com/valyala/fasthttp"
)

func main() {
	// Initialize the database.
	handlers.Database.InitDatabase();

	// Create a router to route requests to the correct handler.
	requestRouter := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Method()) {
			case fasthttp.MethodPut:
				routes.BucketUpload(ctx);
			default:
				ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed);
		}
	}

	server := &fasthttp.Server{
        Handler: requestRouter,
		StreamRequestBody: true,
        MaxRequestBodySize: 1 * 1024 * 1024, // 100 MB
    };

	// Setup HTTP server and listen for requests.
	log.Println("Listening for requests on port 3000");
	if err := server.ListenAndServe(":3000"); err != nil {
		log.Fatal(err);
	}
}