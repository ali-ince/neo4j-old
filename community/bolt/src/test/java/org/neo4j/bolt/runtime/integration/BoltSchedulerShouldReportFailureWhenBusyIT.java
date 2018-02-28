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
package org.neo4j.bolt.runtime.integration;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.concurrent.OtherThreadRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;

@RunWith( Parameterized.class )
public class BoltSchedulerShouldReportFailureWhenBusyIT
{
    private AssertableLogProvider internalLogProvider = new AssertableLogProvider();
    private AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), fsRule::get, getSettingsFunction() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fsRule ).around( server );
    @Rule
    public OtherThreadRule<Integer> spawnedUpdate1 = new OtherThreadRule<>();
    @Rule
    public OtherThreadRule<Integer> spawnedUpdate2 = new OtherThreadRule<>();

    @Parameterized.Parameter( 0 )
    public Factory<TransportConnection> cf;

    @Parameterized.Parameter( 1 )
    public HostnamePort address;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return asList(
                new Object[]{
                        (Factory<TransportConnection>) SocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<TransportConnection>) WebSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<TransportConnection>) SecureSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<TransportConnection>) SecureWebSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                } );
    }

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setInternalLogProvider( internalLogProvider );
        factory.setUserLogProvider( userLogProvider );
        return factory;
    }

    protected Consumer<Map<String,String>> getSettingsFunction()
    {
        return settings ->
        {
            settings.put( GraphDatabaseSettings.auth_enabled.name(), "false" );
            settings.put( new BoltConnector( "bolt" ).enabled.name(), "TRUE" );
            settings.put( new BoltConnector( "bolt" ).listen_address.name(), "localhost:7687" );
            settings.put( new BoltConnector( "bolt" ).type.name(), BoltConnector.ConnectorType.BOLT.name() );
            settings.put( new BoltConnector( "bolt" ).thread_pool_core_size.name(), "0" );
            settings.put( new BoltConnector( "bolt" ).thread_pool_max_size.name(), "2" );
        };
    }

    @Test
    public void shouldReportFailureWhenAllThreadsInThreadPoolAreBusy() throws Exception
    {
        TransportConnection connection1 = performHandshake( cf.newInstance() );
        TransportConnection connection2 = performHandshake( cf.newInstance() );
        TransportConnection connection3 = performHandshake( cf.newInstance() );
        TransportConnection connection4 = performHandshake( cf.newInstance() );

        // Generate a Lock
        createNode( connection1, 100 );
        // Start update request
        updateNode( connection1, 100, 101 );

        // Try to update the same node, these two lines will block all available threads
        Future<Integer> result1 = spawnedUpdate1.execute( state -> updateNodeNoThrow( connection2, 100, 101 ) );
        Future<Integer> result2 = spawnedUpdate2.execute( state -> updateNodeNoThrow( connection3, 100, 101 ) );

        spawnedUpdate1.get().awaitStartExecuting();
        spawnedUpdate2.get().awaitStartExecuting();

        connection4.send( TransportTestUtil.chunk( run( "RETURN 1" ), pullAll() ) );
        assertThat( connection4,
                TransportTestUtil.eventuallyReceives( msgFailure( Status.Request.NoThreadsAvailable, "There are no available threads to serve this request at the moment" ),
                        msgFailure( Status.Request.NoThreadsAvailable, "There are no available threads to serve this request at the moment" ) ) );

        userLogProvider.assertContainsMessageContaining( "since there are no available threads to serve it at the moment. You can retry at a later time" );
        internalLogProvider.assertAtLeastOnce( AssertableLogProvider.inLog( startsWith( BoltConnection.class.getPackage().getName() ) ).error(
                containsString( "since there are no available threads to serve it at the moment. You can retry at a later time" ),
                isA( RejectedExecutionException.class ) ) );
    }

    private TransportConnection performHandshake( TransportConnection connection ) throws Exception
    {
        connection.connect( address ).send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) ).send( TransportTestUtil.chunk( init( "TestClient/1.1", emptyMap() ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( connection, TransportTestUtil.eventuallyReceives( msgSuccess() ) );

        return connection;
    }

    private void createNode( TransportConnection connection, int id ) throws Exception
    {
        connection.send( TransportTestUtil.chunk( run( "BEGIN" ), pullAll(), run( "CREATE (n { id: {id} })", MapUtil.map( "id", id ) ), pullAll(),
                run( "COMMIT" ), pullAll() ) );

        assertThat( connection, TransportTestUtil.eventuallyReceives( msgSuccess(), msgSuccess(), msgSuccess(), msgSuccess(), msgSuccess(), msgSuccess() ) );
    }

    private void updateNode( TransportConnection connection, int oldId, int newId ) throws Exception
    {
        connection.send( TransportTestUtil.chunk( run( "BEGIN" ), pullAll(),
                run( "MATCH (n { id: {oldId} }) SET n.id = {newId}", MapUtil.map( "oldId", oldId, "newId", newId ) ), pullAll() ) );

        assertThat( connection, TransportTestUtil.eventuallyReceives( msgSuccess(), msgSuccess(), msgSuccess(), msgSuccess() ) );
    }

    private int updateNodeNoThrow( TransportConnection connection, int oldId, int newId )
    {
        try
        {
            updateNode( connection, oldId, newId );
        }
        catch ( Throwable t )
        {
            return -1;
        }

        return 0;
    }
}
