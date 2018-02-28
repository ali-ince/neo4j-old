/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.runtime;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.BoltSchedulerProvider;
import org.neo4j.bolt.runtime.CachedThreadPoolExecutorFactory;
import org.neo4j.bolt.runtime.DefaultBoltConnectionFactory;
import org.neo4j.bolt.runtime.ExecutorBoltSchedulerProvider;
import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.v1.messaging.BoltMessageRouter;
import org.neo4j.bolt.v1.messaging.BoltResponseMessageHandler;
import org.neo4j.bolt.v1.messaging.message.RequestMessage;
import org.neo4j.bolt.v1.runtime.spi.Record;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLog;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.SUCCESS;
import static org.neo4j.bolt.v1.messaging.message.DiscardAllMessage.discardAll;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.helpers.collection.MapUtil.map;

public class ResetFuzzTest
{
    private static final String CONNECTOR = "bolt";
    // Because RESET has a "call ahead" mechanism where it will interrupt
    // the session before RESET arrives in order to purge any statements
    // ahead in the message queue, we use this test to convince ourselves
    // there is no code path where RESET causes a session to not go back
    // to a good state.

    private final int seed = new Random().nextInt();

    private final Random rand = new Random( seed );
    private final LifeSupport life = new LifeSupport();
    /** We track the number of un-closed transactions, and fail if we ever leak one */
    private final AtomicLong liveTransactions = new AtomicLong();
    private final Monitors monitors = new Monitors();
    private final Neo4jJobScheduler scheduler = life.add(new Neo4jJobScheduler());
    private final BoltSchedulerProvider boltSchedulerProvider = life.add(
        new ExecutorBoltSchedulerProvider( createConfig(), new CachedThreadPoolExecutorFactory( NullLog.getInstance() ), scheduler,
                NullLogService.getInstance() ) );
    private final Clock clock = Clock.systemUTC();
    private final BoltStateMachine machine = new BoltStateMachine( new FuzzStubSPI(), clock, NullLogService.getInstance() );
    private final BoltConnectionFactory connectionFactory =
            new DefaultBoltConnectionFactory( ( boltChannel, clock ) -> machine, boltSchedulerProvider, NullLogService.getInstance(), clock, null, monitors );
    private BoltChannel boltChannel;

    private final List<List<RequestMessage>> sequences = asList(
            asList( run( "test", map() ), discardAll() ),
            asList( run( "test", map() ), pullAll() ),
            singletonList( run( "test", map() ) )
    );

    private final List<RequestMessage> sent = new LinkedList<>();

    @Before
    public void setup()
    {
        boltChannel = mock( BoltChannel.class );
        when( boltChannel.id() ).thenReturn( UUID.randomUUID().toString() );
        when( boltChannel.connector() ).thenReturn( CONNECTOR );
    }

    @Test
    public void shouldAlwaysReturnToReadyAfterReset() throws Throwable
    {
        // given
        life.start();
        BoltConnection boltConnection = connectionFactory.newConnection( boltChannel );
        boltConnection.start();
        boltConnection.enqueue( session -> session.init( "ResetFuzzTest/0.0", Collections.emptyMap(), nullResponseHandler() ) );

        BoltMessageRouter router = new BoltMessageRouter(
                NullLog.getInstance(), boltConnection, new BoltResponseMessageHandler<IOException>()
        {
            @Override
            public void onRecord( Record item ) throws IOException
            {
            }

            @Override
            public void onIgnored() throws IOException
            {
            }

            @Override
            public void onFailure( Status status, String message ) throws IOException
            {
            }

            @Override
            public void onSuccess( Map metadata ) throws IOException
            {
            }
        }, () -> {} );

        // Test random combinations of messages within a small budget of testing time.
        long deadline = System.currentTimeMillis() + 2 * 1000;

        // when
        while ( System.currentTimeMillis() < deadline )
        {
            dispatchRandomSequenceOfMessages( router );
            assertSchedulerWorks( boltConnection );
        }
    }

    private void assertSchedulerWorks( BoltConnection connection ) throws InterruptedException
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        connection.enqueue( machine -> machine.reset( recorder ) );

        try
        {
            RecordedBoltResponse response = recorder.nextResponse();
            assertThat( SUCCESS, equalTo( response.message() ) );
            assertThat( machine.state(), equalTo( BoltStateMachine.State.READY ) );
            assertThat( liveTransactions.get(), equalTo( 0L ) );
        }
        catch ( AssertionError e )
        {
            throw new AssertionError( String.format( "Expected session to return to good state after RESET, but " +
                                                     "assertion failed: %s.%n" +
                                                     "Seed: %s%n" +
                                                     "Messages sent:%n" +
                                                     "%s",
                    e.getMessage(), seed, Iterables.toString( sent, "\n" ) ), e );
        }
    }

    private void dispatchRandomSequenceOfMessages( BoltMessageRouter messageHandler )
    {
        List<RequestMessage> sequence = sequences.get( rand.nextInt( sequences.size() ) );
        for ( RequestMessage message : sequence )
        {
            sent.add( message );
            message.dispatch( messageHandler );
        }
    }

    @After
    public void cleanup()
    {
        life.shutdown();
    }

    private static Config createConfig()
    {
        Map<String, String> configProps = new HashMap<>();

        configProps.put( new BoltConnector( CONNECTOR ).enabled.name(), "TRUE" );
        configProps.put( new BoltConnector( CONNECTOR ).listen_address.name(), "localhost:0" );
        configProps.put( new BoltConnector( CONNECTOR ).type.name(), BoltConnector.ConnectorType.BOLT.name() );
        configProps.put( new BoltConnector( CONNECTOR ).thread_pool_core_size.name(), "5" );
        configProps.put( new BoltConnector( CONNECTOR ).thread_pool_max_size.name(), "10" );

        return Config.serverDefaults( configProps );
    }
    /**
     * We can't use mockito to create this, because it stores all invocations,
     * so we run out of RAM in like five seconds.
     */
    private class FuzzStubSPI implements BoltStateMachine.SPI
    {
        @Override
        public BoltConnectionDescriptor connectionDescriptor()
        {
            return boltChannel;
        }

        @Override
        public void register( BoltStateMachine machine, String owner )
        {
            // do nothing
        }

        @Override
        public TransactionStateMachine.SPI transactionSpi()
        {
            return null;
        }

        @Override
        public void onTerminate( BoltStateMachine machine )
        {
            // do nothing
        }

        @Override
        public void reportError( Neo4jError err )
        {
            // do nothing
        }

        @Override
        public AuthenticationResult authenticate( Map<String,Object> authToken ) throws AuthenticationException
        {
            return AuthenticationResult.AUTH_DISABLED;
        }

        @Override
        public void udcRegisterClient( String clientName )
        {
            // do nothing
        }

        @Override
        public String version()
        {
            return "<test-version>";
        }
    }
}
