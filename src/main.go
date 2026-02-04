package main

import (
	"log"
	"speedyvault/src/handlers"
	"speedyvault/src/routes"

	"github.com/valyala/fasthttp"
)

var server *fasthttp.Server;

func main() {
	// Initialize the database.
	handlers.Database.InitDatabase();

	// Create a router to route requests to the correct handler.
	requestRouter := func(ctx *fasthttp.RequestCtx) {
		if ctx.IsPut() {
			routes.BucketUpload(ctx);
		} else if ctx.IsGet() {
			if len(ctx.Request.Header.Peek("content-length")) != 0 {
				ctx.Error("body not allowed in GET requests", 400);
				ctx.SetConnectionClose();
				return;
			}

			routes.ObjectDownload(ctx);
		} else {
			ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed);
		}
	};

	server = &fasthttp.Server{
        Handler: requestRouter,
		StreamRequestBody: true,
		//WriteBufferSize: 2,
        MaxRequestBodySize: 1, // 100 MB
    };
	
	// Setup HTTP server and listen for requests.
	log.Println("Listening for requests on port 3000");
	if err := server.ListenAndServe(":3000"); err != nil {
		log.Fatal(err);
	}
}