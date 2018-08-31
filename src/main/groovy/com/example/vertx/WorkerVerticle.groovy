package com.example.vertx

import io.vertx.core.Future
import io.vertx.core.logging.LoggerFactory
import io.vertx.core.json.JsonObject
import io.vertx.reactivex.core.AbstractVerticle
import io.vertx.reactivex.core.eventbus.EventBus


class WorkerVerticle extends AbstractVerticle {
    private final Random random = new Random()
    def log = LoggerFactory.getLogger(this.class)

    @Override
    void start(Future<Void> future) {
        EventBus eb = this.@vertx.eventBus()
        eb.consumer('process.coffee.order').toFlowable().subscribe({ message ->
            def body = message.body()
            log.info("Processing coffee order ${body.getString('id')} for ${body.getString('customer')}: ${body.getString('coffee')}, ${body.getString('size')}, instance:${this}")
            Thread.sleep(random.nextInt(10) * 1000)
            log.info("Order ${body.getString('id')} complete! Calling ${body.getString('customer')} instance:${this}")
            def response = new JsonObject()
            response.put('id', body.getString('id'))
            response.put('success', true)
            message.reply(response)
        })
    }
}
