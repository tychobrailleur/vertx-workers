package com.example.vertx

import io.reactivex.functions.Consumer
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.reactivex.core.AbstractVerticle
import io.vertx.reactivex.core.Vertx
import io.vertx.reactivex.core.buffer.Buffer
import io.vertx.reactivex.ext.web.Router
import io.vertx.reactivex.ext.web.RoutingContext
import io.vertx.reactivex.ext.web.handler.BodyHandler

class MainVerticle extends AbstractVerticle {
    def log = LoggerFactory.getLogger(this.class)

    @Override
    void start(Future<Void> future) {
        JsonObject config = config()
        int port = config.getInteger('http.port', 8085)
        log.info("Starting MainVerticle on port ${port} context = ${Vertx.currentContext()}")

        def server = this.@vertx.createHttpServer()

        Router router = Router.router(this.@vertx)
        router.post().handler(BodyHandler.create())
        router.post('/coffee').handler(this.&processCoffeeOrder)

        server
            .requestHandler(router.&accept)
            .rxListen(port)
            .subscribe({ httpServer -> log.info("server is running...") } as Consumer)
    }

    private void processCoffeeOrder(RoutingContext routingContext) {
        def requestBody = routingContext.getBodyAsJson()
        log.info("Processing request... ${requestBody} context = ${Vertx.currentContext()}")

        def eb = this.@vertx.eventBus()

        def message = new JsonObject()
        def correlationId = UUID.randomUUID().toString()
        message.put('id', correlationId)
        message.put('customer', requestBody?.getString('customer'))
        message.put('coffee', requestBody?.getString('coffee'))
        message.put('size', requestBody?.getString('size'))


        eb.rxSend('process.coffee.order', message).map({ it.body() }).subscribe({ responseHandler ->
            log.info("Coffee processed successfully.")
            def responseMessage = new JsonObject()

            responseMessage.put("order", "ok")
            responseMessage.put('id', correlationId)

            routingContext.response().setChunked(true)
                .putHeader("Content-Type", "application/json")
                .end(new Buffer(responseMessage.toBuffer()))
        } as Consumer)
    }
}
