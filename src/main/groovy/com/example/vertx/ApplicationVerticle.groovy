package com.example.vertx

import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory

class ApplicationVerticle extends AbstractVerticle {

    @Override
    public void start() {
        def log = LoggerFactory.getLogger(this.class)

        ConfigStoreOptions fileStore = new ConfigStoreOptions()
            .setType("file")
            .setOptional(true)
            .setConfig(new JsonObject().put("path", "conf/config.json"))

        ConfigStoreOptions sysPropsStore = new ConfigStoreOptions().setType("sys")

        ConfigRetrieverOptions options = new ConfigRetrieverOptions()
            .addStore(fileStore)
            .addStore(sysPropsStore)

        ConfigRetriever retriever = ConfigRetriever.create(this.@vertx, options)

        retriever.getConfig({ json ->
            if (json.succeeded()) {
                JsonObject config = json.result()
                log.debug("Starting the app with config: ${json}")
                this.@vertx.deployVerticle('groovy:com.example.vertx.MainVerticle', new DeploymentOptions()
                    .setConfig(config)
                    .setInstances(5)
                )
                this.@vertx.deployVerticle('groovy:com.example.vertx.WorkerVerticle', new DeploymentOptions()
                    .setConfig(config)
                    .setWorkerPoolName("coffee-making-pool")
                    .setWorkerPoolSize(5)
                //         .setMultiThreaded(true)
                    .setInstances(5)
                    .setWorker(true)
                )
            } else {
                log.error("Error retrieving configuration.")
            }
        })
    }
}
