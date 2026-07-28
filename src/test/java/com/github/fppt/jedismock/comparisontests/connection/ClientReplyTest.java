package com.github.fppt.jedismock.comparisontests.connection;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CLIENT REPLY ON/OFF/SKIP semantics. Suppression applies to command replies
 * (including error replies and the +OK of OFF/SKIP themselves) but never to
 * pub/sub pushes such as subscribe acknowledgements. Asserted over a raw
 * socket because suppressed replies are invisible to a request/response
 * client, and because the interesting property is exactly which bytes arrive.
 */
@ExtendWith(ComparisonBase.class)
public class ClientReplyTest {

    static class RawClient implements AutoCloseable {
        private final Socket socket;
        private final OutputStream out;
        private final DataInputStream in;

        RawClient(HostAndPort hostAndPort) throws IOException {
            socket = new Socket(hostAndPort.getHost(), hostAndPort.getPort());
            socket.setSoTimeout(5000);
            out = socket.getOutputStream();
            in = new DataInputStream(socket.getInputStream());
        }

        void send(String... args) throws IOException {
            StringBuilder command = new StringBuilder("*").append(args.length).append("\r\n");
            for (String arg : args) {
                command.append('$').append(arg.getBytes(StandardCharsets.UTF_8).length).append("\r\n")
                        .append(arg).append("\r\n");
            }
            out.write(command.toString().getBytes(StandardCharsets.UTF_8));
            out.flush();
        }

        /**
         * Reads exactly as many bytes as the expected replies occupy: anything
         * suppressed by CLIENT REPLY simply never arrives, so the next reply's
         * bytes follow immediately.
         */
        String read(int length) throws IOException {
            byte[] buffer = new byte[length];
            in.readFully(buffer);
            return new String(buffer, StandardCharsets.UTF_8);
        }

        @Override
        public void close() throws Exception {
            socket.close();
        }
    }

    @TestTemplate
    public void offSuppressesRepliesIncludingErrorsUntilOn(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (RawClient client = new RawClient(hostAndPort)) {
            client.send("CLIENT", "REPLY", "OFF");        //+OK suppressed
            client.send("SET", "reply_key", "v");         //+OK suppressed
            client.send("INCR", "reply_key");             //-ERR suppressed
            client.send("CLIENT", "REPLY", "ON");         //replies +OK
            client.send("ECHO", "fin");
            String expected = "+OK\r\n$3\r\nfin\r\n";
            assertThat(client.read(expected.length())).isEqualTo(expected);
        }
    }

    @TestTemplate
    public void skipSuppressesExactlyTheNextReply(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (RawClient client = new RawClient(hostAndPort)) {
            client.send("CLIENT", "REPLY", "SKIP");       //+OK suppressed
            client.send("SET", "reply_key", "v");         //the "next" reply: suppressed
            client.send("ECHO", "one");                   //replies again
            String expected = "$3\r\none\r\n";
            assertThat(client.read(expected.length())).isEqualTo(expected);
        }
    }

    @TestTemplate
    public void skipIsANoOpWhileRepliesAreOff(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (RawClient client = new RawClient(hostAndPort)) {
            client.send("CLIENT", "REPLY", "OFF");
            client.send("CLIENT", "REPLY", "SKIP");       //no-op in OFF mode
            client.send("CLIENT", "REPLY", "ON");         //replies +OK, not skipped
            client.send("ECHO", "x");
            String expected = "+OK\r\n$1\r\nx\r\n";
            assertThat(client.read(expected.length())).isEqualTo(expected);
        }
    }

    @TestTemplate
    public void subscribeAcknowledgementsBypassSuppression(Jedis jedis, HostAndPort hostAndPort) throws Exception {
        try (RawClient client = new RawClient(hostAndPort)) {
            client.send("CLIENT", "REPLY", "OFF");        //+OK suppressed
            client.send("SUBSCRIBE", "reply_channel");    //the ack is a push, not a reply
            String expected = "*3\r\n$9\r\nsubscribe\r\n$13\r\nreply_channel\r\n:1\r\n";
            assertThat(client.read(expected.length())).isEqualTo(expected);
        }
    }
}
