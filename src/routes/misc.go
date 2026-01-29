package routes

import (
	"github.com/valyala/fasthttp"
)

func Ping(ctx* fasthttp.RequestCtx) {
	ctx.Response.SetStatusCode(fasthttp.StatusNoContent);
}