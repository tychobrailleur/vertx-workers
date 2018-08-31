package com.example.vertx

import io.reactivex.functions.Consumer
import io.vertx.core.Future
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.reactivex.core.AbstractVerticle
import io.vertx.reactivex.core.buffer.Buffer

class MainVerticle extends AbstractVerticle {
    def log = LoggerFactory.getLogger(this.class)

    @Override
    void start(Future<Void> future) {
        JsonObject config = config()
        int port = config.getInteger('http.port', 8085)
        log.info("Starting MainVerticle on port ${port}")

        def server = this.@vertx.createHttpServer()
        server.requestStream().toFlowable().subscribe({ request ->
            def response = request.response()
            response.setChunked(true)
            response.putHeader("Content-Type", "application/json")
            request.toFlowable().subscribe(
                { buffer ->
                    processCoffeeOrder(buffer, response)
                },
                { err ->
                    log.error("Error processing", err)
                })
        })
        server.listen(port)
    }

    private void processCoffeeOrder(Buffer buffer, def response) {
        def requestBody = buffer.toJsonObject()
        log.info("Processing request... ${requestBody} [response = ${response}]")

        def eb = this.@vertx.eventBus()

        def message = new JsonObject()
        def correlationId = UUID.randomUUID().toString()
        message.put('id', correlationId)
        message.put('customer', requestBody.getString('customer'))
        message.put('coffee', requestBody.getString('coffee'))
        message.put('size', requestBody.getString('size'))

        eb.rxSend('process.coffee.order', message).subscribe({ responseHandler ->
            log.info("Coffee processed successfully.")
            def responseMessage = new JsonObject()

            responseMessage.put("order", "ok")
            responseMessage.put('id', correlationId)

            Buffer buf = new Buffer(responseMessage.toBuffer())
            response.write(buf)
            response.end()
        } as Consumer, { responseHandler ->
            response.fail(500)
        })
    }
}
