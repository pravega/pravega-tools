package io.pravega.tools.pravegacli.commands;

import io.pravega.common.Exceptions;
import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.curator.framework.CuratorFramework;

public abstract class ControllerCommand extends Command {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    ControllerCommand(CommandArgs args) {
        super(args);
    }

    protected ControllerCommand.Context createContext() {
        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        Client client = ClientBuilder.newClient(clientConfig);

        return new Context(client, null);
    }

    @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
    protected static class Context implements AutoCloseable {
        private final Client client;
        private final WebTarget webTarget;

        @Override
        @SneakyThrows(BKException.class)
        public void close() {
            this.client.close();
        }
    }
}
